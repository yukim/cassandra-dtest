from dtest import Tester

import os, sys, time
from ccmlib.cluster import Cluster

class TestDeletion(Tester):

    def gc_test(self):
        """ Test that tombstone are fully purge after gc_grace """
        cluster = self.cluster

        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        time.sleep(.5)
        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf', gc_grace=0, key_type='int', columns={'c1': 'int'})

        cursor.execute('insert into cf (key, c1) values (1,1)')
        cursor.execute('insert into cf (key, c1) values (2,1)')
        node1.flush()

        result = cursor.execute('select * from cf;')
        assert len(result) == 2 and len(result[0]) == 2 and len(result[1]) == 2, result

        cursor.execute('delete from cf where key=1')
        result = cursor.execute('select * from cf;')
        if cluster.version() < '1.2': # > 1.2 doesn't show tombstones
            assert len(result) == 2 and len(result[0]) == 1 and len(result[1]) == 1, result

        node1.flush()
        time.sleep(.5)
        node1.compact()
        time.sleep(.5)

        result = cursor.execute('select * from cf;')
        assert len(result) == 1 and len(result[0]) == 2, result

