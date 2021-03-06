import random, time
from dtest import debug, Tester
from tools import new_node, insert_c1c2, query_c1c2
from assertions import assert_almost_equal
from ccmlib.cluster import Cluster
from cassandra import ConsistencyLevel
from decimal import Decimal


def get_size(node):
    """Uses nodetool info to get load on node"""
    info = node.nodetool('info', capture_output=True)[0]
    load_line = filter(lambda s: s.startswith('Load'),
                       info.split('\n'))[0].split()
    load_num, load_units = load_line[2], load_line[3]
    # no unit conversions, so enforce consistent units
    assert load_units == 'KB'
    return Decimal(load_num)


class TestBootstrap(Tester):

    def __init__(self, *args, **kwargs):
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]
        Tester.__init__(self, *args, **kwargs)

    def simple_bootstrap_test(self):
        cluster = self.cluster
        tokens = cluster.balanced_tokens(2)

        keys = 10000

        # Create a single node cluster
        cluster.populate(1, tokens=[tokens[0]]).start(wait_other_notice=True)
        node1 = cluster.nodes["node1"]

        debug('connecting...')
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', columns={ 'c1' : 'text', 'c2' : 'text' })

        for n in xrange(0, keys):
            insert_c1c2(session, n, ConsistencyLevel.ONE)

        node1.flush()
        initial_size = get_size(node1)

        # Reads inserted data all during the boostrap process. We shouldn't
        # get any error
        reader = self.go(lambda _: query_c1c2(session, random.randint(0, keys-1), ConsistencyLevel.ONE))

        # Boostraping a new node
        node2 = new_node(cluster, token=tokens[1])
        node2.start(wait_for_binary_proto=True)

        debug('reading...')
        reader.check()
        node1.cleanup()
        time.sleep(.5)
        reader.check()

        size1, size2 = get_size(node1), get_size(node2)

        debug('initial_size: {}'.format(initial_size))
        debug('size1: {}'.format(size1))
        debug('size2: {}'.format(size2))

        assert_almost_equal(size1, size2, error=0.3)
        assert_almost_equal(initial_size, 2 * size1)


    def read_from_bootstrapped_node_test(self):
        """Test bootstrapped node sees existing data, eg. CASSANDRA-6648"""
        cluster = self.cluster
        cluster.populate(3)
        version = cluster.version()
        cluster.start()

        node1 = cluster.nodes['node1']
        if version < "2.1":
            node1.stress(['-n', '10000'])
        else:
            node1.stress(['write', 'n=10000', '-rate', 'threads=8'])

        node4 = new_node(cluster)
        node4.start()

        session = self.patient_cql_connection(node4)
        stress_table = 'keyspace1.standard1' if self.cluster.version() >= '2.1' else '"Keyspace1"."Standard1"'
        rows = session.execute('select * from %s limit 10' % stress_table)
        assert len(list(rows)) == 10
