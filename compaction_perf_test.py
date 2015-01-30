from dtest import Tester, debug, DISABLE_VNODES
import unittest
from ccmlib.cluster import Cluster
from ccmlib.node import Node, NodeError, TimeoutError
from re import findall
from assertions import assert_invalid, assert_all, assert_none, assert_one
import tempfile
import os

class TestCompactionPerf(Tester):

    def write_only_workload(self, node, compstrat):
        path = os.path.dirname(os.path.abspath(__file__)) + "wronly.txt"
        node.stress(["write", "n=1000000", "-pop", "seq=1..1M", "-schema", "compaction(strategy="+compstrat+")", "-log", "file=" + path])
        stats = self.get_stress_stats_from_file(path)
        return stats

    def read_update_workload(self, node, compstrat):
        path = os.path.dirname(os.path.abspath(__file__)) + "wronly.txt"
        node.stress(["mixed", "n=1000000", "ratio(read=3,write=1)", "-insert", "visits=EXP(1..5)", "partitions=fixed(1)", "select-ratio=fixed(1)/1", "-pop", "seq=1M..2M", "-schema", "compaction(strategy="+compstrat+")", "-log", "file=" + path])
        stats = self.get_stress_stats_from_file(path)
        return stats

    def get_stress_stats_from_file(self, filename):
        with open(filename, 'r') as f:
            output = f.read()
        oprate = findall('(?<=op rate).*', output)[0].strip()[2:]
        lat_mean = findall('(?<=latency mean).*', output)[0].strip()[2:]
        lat_95 = findall('(?<=latency 95th percentile).*', output)[0].strip()[2:]
        lat_99 = findall('(?<=latency 99th percentile).*', output)[0].strip()[2:]
        lat_999 = findall('(?<=latency 99.9th percentile).*', output)[0].strip()[2:]

        return [float(oprate), float(lat_99), float(lat_999)]

    def compaction_perf_test(self):
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)

        strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy']

        wronly = []
        rdupdate = []

        for strat in strategies:
            wores = self.write_only_workload(node1, strat)
            wronly.append(wores)

            rures = self.read_update_workload(node1, strat)
            rdupdate.append(rures)

            cursor.execute("drop keyspace keyspace1;")

        #TO-DO find meaninful way of comparing
        #TO-DO complete time-series
