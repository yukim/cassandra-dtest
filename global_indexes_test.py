from dtest import Tester
from assertions import assert_one, assert_invalid, assert_none
from cassandra import InvalidRequest, ConsistencyLevel
from cassandra.query import SimpleStatement
import re
from tools import since, new_node

class TestGlobalIndexes(Tester):

    def prepare(self):
        cluster = self.cluster

        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        return session

    @since('3.0')
    def populate_index_after_insert_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")

        for i in xrange(10000):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        session.execute("CREATE INDEX ON t (v)")

        #Should not be able to query an index until it is built.
        for i in xrange(10000):
            try:
                assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])
            except InvalidRequest as e:
                assert re.search("Cannot query index until it is built.", str(e))

    @since('3.0')
    def drop_index_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute("CREATE INDEX ON t (v)")

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])

        session.execute("DROP INDEX ks.t_v_idx")

        for i in xrange(1000):
            assert_invalid(session, "SELECT * FROM t WHERE v = %d" % i)

    @since('3.0')
    def test_6924_dropping_cf(self):
        session = self.prepare()

        for i in xrange(10):
            try:
                session.execute("DROP TABLE t")
            except InvalidRequest:
                pass

            session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
            session.execute("CREATE INDEX ON t (v)")

            for i in xrange(10):
                session.execute("INSERT INTO t (id, v) VALUES (%d, 0)" % i)

            rows = session.execute("select count(*) from t WHERE v=0")
            count = rows[0][0]
            self.assertEqual(count, 10)

    @since('3.0')
    def add_node_after_index_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute("CREATE INDEX ON t (v)")

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])

        node4 = new_node(self.cluster)
        node4.start()

        session2 = self.patient_exclusive_cql_connection(node4)

        for i in xrange(1000):
            assert_one(session2, "SELECT * FROM ks.t WHERE v = %d" % i, [i, i])

        for i in xrange(1000, 1100):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        for i in xrange(1000, 1100):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])

    @since('3.0')
    def drop_node_after_index_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute("CREATE INDEX ON t (v)")

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])

        node3 = self.cluster.nodelist()[2]
        node3.nodetool("decommission")
        self.cluster.remove(node3)

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM ks.t WHERE v = %d" % i, [i, i])

        for i in xrange(1000, 1100):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        for i in xrange(1000, 1100):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])

    @since('3.0')
    def add_dc_after_index_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute("CREATE INDEX ON t (v)")

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])

        node4 = new_node(self.cluster, data_center='dc2')
        node4.start()
        node5 = new_node(self.cluster, remote_debug_port='2500', data_center='dc2')
        node5.start()

        session2 = self.patient_exclusive_cql_connection(node4)

        for i in xrange(1000):
            assert_one(session2, "SELECT * FROM ks.t WHERE v = %d" % i, [i, i])

        for i in xrange(1000, 1100):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        for i in xrange(1000, 1100):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])

    @since('3.0')
    def test_8272(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v text)")
        session.execute("CREATE INDEX ON t (v)")

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES (0, 'foo%s')" % i)

        for i in xrange(1000):
            insert_query = SimpleStatement("UPDATE t SET v = 'bar%s' WHERE id = 0" % i, ConsistencyLevel.QUORUM)

            session.execute(insert_query)
            assert_none(session, "SELECT * from t WHERE v = 'foo%s'" % i, ConsistencyLevel.QUORUM)


    ##BASIC TESTS. COPY 2i tests from cql_tests here

    @since('3.0')
    def global_index_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute("CREATE INDEX ON t (v)")

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])