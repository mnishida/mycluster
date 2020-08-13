#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from nose.tools import assert_equal
dirname = os.path.dirname(__file__)


def test_hostsfile():
    import os
    dirname = os.path.dirname(__file__)
    from mycluster import Cluster
    from ipyparallel import Client
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
    cluster = Cluster(hostsfile)
    assert_equal(hosts, cluster.hosts)
    cluster.start_controller()
    cluster.start_engines()
    rc = Client(profile="ssh")
    assert_equal(len(rc.ids), 2)
    cluster.shutdown()


def test_environ():
    from mycluster import Cluster
    from ipyparallel import Client
    hosts = [
        ('localhost', 2, 1),
    ]
    cluster = Cluster(hosts)
    cluster.start_controller()
    cluster.start_engines()
    rc = Client(profile="ssh")
    dview = rc[:]
    dview.block = True
    print(rc.ids)
    dview.execute('import os; num_threads = os.environ.get("OMP_NUM_THREADS")')
    onts = dview['num_threads']
    print(onts)
    assert_equal(onts, ['1', '1'])
    cluster.shutdown()
