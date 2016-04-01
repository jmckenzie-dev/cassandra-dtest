import os
import os.path
from dtest import Tester, DISABLE_VNODES
from tools import new_node
from assertions import assert_almost_equal
from jmxutils import JolokiaAgent, make_mbean, remove_perf_disable_shared_mem


class TestRangeAwareCompaction(Tester):

    def basic_test(self):
        cluster = self.cluster
        cluster.set_datadir_count(1)
        cluster.populate(3).start(wait_for_binary_proto=True)
        [node1, node2, node3] = cluster.nodelist()

        node1.stress(['write', 'n=1000000', '-rate', 'threads=100', '-schema',
                      'compaction(strategy=SizeTieredCompactionStrategy,range_aware_compaction=true,min_range_sstable_size_in_mb=0) replication(factor=3)'])
        node1.flush()

        node1.compact()  # note that we still need to compact twice, first to get everything out of L0, then combine the sstables in "L1"
        node1.compact()
        if DISABLE_VNODES:
            self.assertEqual(len(node1.get_sstables('keyspace1', 'standard1')), 3)
        else:
            # +1 is due to the fact that we unwrap the last range
            self.assertEqual(len(node1.get_sstables('keyspace1', 'standard1')), int(node1.get_conf_option('num_tokens')) * 3 + 1)

    def bootstrap_test(self):
        cluster = self.cluster
        cluster.set_datadir_count(3)
        cluster.populate(3).start(wait_for_binary_proto=True)
        [node1, node2, node3] = cluster.nodelist()

        node1.stress(['write', 'n=10000', '-rate', 'threads=100', '-schema',
                      'compaction(strategy=SizeTieredCompactionStrategy,range_aware_compaction=true,min_range_sstable_size_in_mb=0) replication(factor=3)'])
        cluster.flush()

        for node in cluster.nodelist():
            node.compact()

        node4 = new_node(cluster)
        node4.start(wait_for_binary_proto=True)
        per_directory_sums = []
        for x in node4.get_sstables_per_data_directory('keyspace1', 'standard1'):
            sum = 0
            for y in x:
                sum += os.path.getsize(y)
            per_directory_sums.append(sum)

        assert_almost_equal(*per_directory_sums)

    def decommission_test(self):
        cluster = self.cluster
        cluster.set_datadir_count(3)
        cluster.populate(4).start(wait_for_binary_proto=True)
        [node1, node2, node3, node4] = cluster.nodelist()

        node1.stress(['write', 'n=10000', '-rate', 'threads=100', '-schema',
                      'compaction(strategy=SizeTieredCompactionStrategy,range_aware_compaction=true,min_range_sstable_size_in_mb=0) replication(factor=3)'])
        cluster.flush()

        for node in cluster.nodelist():
            node.compact()

        node2.decommission()

    def userdefined_test(self):
        cluster = self.cluster
        cluster.set_datadir_count(3)
        cluster.populate(1)
        [node1] = cluster.nodelist()
        remove_perf_disable_shared_mem(node1)
        cluster.start(wait_for_binary_proto=True)
        node1.stress(['write', 'n=1M', '-rate', 'threads=100', '-schema',
                      'compaction(strategy=SizeTieredCompactionStrategy,range_aware_compaction=true,min_range_sstable_size_in_mb=0,enabled=false)'])
        node1.flush()
        sstables = node1.get_sstables_per_data_directory('keyspace1', 'standard1')
        sstablescompact = ",".join(sstables[0])
        mbean = make_mbean('db', type='CompactionManager')
        with JolokiaAgent(node1) as jmx:
            jmx.execute_method(mbean, 'forceUserDefinedCompaction', [sstablescompact])
        sstablescompact = "{0},{1}".format(sstables[1][0], sstables[2][0])
        expected_error = "mix sstables from different directories"
        self.ignore_log_patterns = [expected_error]

        got_exception = False
        try:
            with JolokiaAgent(node1) as jmx:
                jmx.execute_method(mbean, 'forceUserDefinedCompaction', [sstablescompact])
        except:
            got_exception = True

        self.assertTrue(got_exception)
        self.assertTrue(len(node1.grep_log(expected_error)) > 0)
        node1.watch_log_for(expected_error)
        got_exception = False
        sstables = node1.get_sstables_per_data_directory('keyspace1', 'standard1')
        try:
            with JolokiaAgent(node1) as jmx:
                jmx.execute_method(mbean, 'forceUserDefinedCompaction', [",".join([sstables[0][1], sstables[0][2]])])
        except:
            got_exception = True
        self.assertTrue(got_exception)
        self.assertTrue(len(node1.grep_log('All sstables are not in the same range-compaction strategy')) > 0)

        with JolokiaAgent(node1) as jmx:
            jmx.execute_method(mbean, 'forceUserDefinedCompaction', [",".join([sstables[0][1]])])
