import time, re
from dtest import Tester, debug
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from tools import no_vnodes, insert_c1c2, query_c1c2

class Test16084(Tester):

    def t16084_test(self):
        cluster = self.cluster

        cluster.populate(2)
        cluster.set_configuration_options(values={
                                            'hinted_handoff_enabled': False,
                                            'enable_christmas_patch': True
                                          },
                                          batch_commitlog=True)
        cluster.start()
        [node1, node2] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        # create keyspace with RF=3
        self.create_ks(cursor, 'ks', 2)
        # create table with low gc grace seconds
        # compaction is disabled not to purge data
        query = """
            CREATE TABLE cf (
                key text,
                c1 text,
                c2 text,
                PRIMARY KEY (key, c1)
            )
            WITH gc_grace_seconds=1
            AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'};
        """
        cursor.execute(query)
        time.sleep(.5)
        # insert data
        query = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('k1', 'v1', 'value')", consistency_level=ConsistencyLevel.ALL)
        cursor.execute(query)
        node1.flush()
        node2.flush()
        # take node2 down, and delete only node1
        node2.stop(wait_other_notice=True)
        query = SimpleStatement("DELETE FROM cf WHERE key='k1'", consistency_level=ConsistencyLevel.ONE)
        cursor.execute(query)
        node1.flush()
        # wait for gc grace
        time.sleep(2)
        # select on node1, should be empty
        query = SimpleStatement("SELECT c1, c2 FROM cf WHERE key='k1'", consistency_level=ConsistencyLevel.ONE)
        res = cursor.execute(query)
        assert len(filter(lambda x: len(x) != 0, res)) == 0, res
        # manually compacting cf does not drop all SSTables since they are not repaired
        # ('Christmas' patch)
        node1.nodetool("compact ks cf")
        sstables = node1.get_sstables("ks", "")
        assert len(sstables) == 1, sstables # 2 sstables compacted to 1
        # bring up node2 and see if select with CL.ALL still returns nothing
        # (https://datastax.zendesk.com/agent/tickets/16084)
        node2.start(wait_other_notice=True)
        query = SimpleStatement("SELECT c1, c2 FROM cf WHERE key='k1'", consistency_level=ConsistencyLevel.ALL)
        res = cursor.execute(query)
        assert len(filter(lambda x: len(x) != 0, res)) == 0, res
        # issue repair and see data is not coming back
        node2.repair(self._repair_options(ks='ks', cf=['cf']))
        query = SimpleStatement("SELECT c1, c2 FROM cf WHERE key='k1'", consistency_level=ConsistencyLevel.ALL)
        res = cursor.execute(query)
        assert len(filter(lambda x: len(x) != 0, res)) == 0, res
        # compact both nodes and see all SSTables are gone
        # ('Christmas' patch)
        for node in [node1, node2]:
            node.nodetool("compact ks cf")
            sstables = node.get_sstables("ks", "")
            assert len(sstables) == 0, sstables

    def _repair_options(self, ks='', cf=[], sequential=True):
        opts = []
        version = self.cluster.version()
        # since version 3.0, default is parallel, otherwise it's sequential
        if sequential:
            if version >= '3.0':
                opts += ['-seq']
        else:
            if version < '3.0':
                opts += ['-par']

        # test with full repair
        if version >= '3.0':
            opts += ['-full']
        if ks:
            opts += [ks]
        if cf:
            opts += cf
        return opts

