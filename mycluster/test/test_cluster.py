#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from nose.tools import assert_equal
dirname = os.path.dirname(__file__)


def test_hostsfile():
    import os
    dirname = os.path.dirname(__file__)
    from mycluster import Cluster
    hosts = [
        ('localhost', 2, 1),
    ]
    hostsfile = os.path.join(dirname, "hosts")
    lines = ["{0} {1} {2}\n".format(host, num, num_threads)
             for host, num, num_threads in hosts]
    with open(hostsfile, "w") as fp:
        fp.writelines(lines)
    with open(hostsfile, "r") as fp:
        for line in lines:
            assert_equal(fp.readline(), line)
    with Cluster(hostsfile) as cluster:
        assert_equal(hosts, cluster.hosts)
        assert_equal(len(cluster.rc.ids), 2)


def test_environ():
    from mycluster import Cluster
    hosts = [
        ('localhost', 2, 1),
    ]
    with Cluster(hosts) as cluster:
        dview = cluster.rc[:]
        dview.block = True
        print(cluster.rc.ids)
        dview.execute('import os; num_threads = os.environ.get("OMP_NUM_THREADS")')
        onts = dview['num_threads']
        print(onts)
        assert_equal(onts, ['1', '1'])
