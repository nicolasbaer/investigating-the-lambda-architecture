#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Torque starter script for `Investigating the lambda architecture`.

Usage:
  starter.py qsub <resource_file> <exec_path>
  starter.py configure_node <resource_file> <nodenum> <vnodenum> <node_file> <exec_path>
  starter.py wait <resource_file> <nodenum> <vnodenum> <node_file> <exec_path>
  starter.py -h | --help

Options:
  -h --help     Show this screen.

"""
__author__ = 'Nicolas Bär <nicolas.baer@uzh.ch'

import collections
import json
import os
import time

from docopt import docopt
import sh


def qsub(resource_file, exec_path):
    """

    :param resource_file: path to json resource file
    """
    res = _read_resource_file(resource_file)

    path = os.path.expandvars(os.path.expanduser(exec_path))

    walltime = res["resource"]["walltime"]

    max_cpus = 0
    nodes = len(res["node"])
    for node, systems in res["node"].iteritems():
        cpus = 0
        for system in systems.itervalues():
            cpus += system['cpus']
        max_cpus = max(cpus, max_cpus)

    sh.qsub(os.path.join(path, "submit_new.sh"),
            N="lambdathesis",
            l="nodes=%s:ppn=%s:walltime=%s" % (nodes, max_cpus, walltime),
            j="oe",
            o="lambda-out",
            e="lambda-err",
            m="n",
            V="True")


def configure_node(resource_file, node_num, v_node_num, node_file, exec_path):
    """

    :param resource_file: path to json resource file
    :param node_num: number of current node
    :param v_node_num: v node number of job
    :param node_file: file with node mapping
    """
    res = _read_resource_file(resource_file)
    mapping = _read_node_service_mapping(res, node_file)

    path = os.path.expandvars(os.path.expanduser(exec_path))

    services = res["node"][node_num]
    for name, settings in services.iteritems():
        if name == "zookeeper":
            print "starting zookeeper"
            sh.bash("zookeeper.sh")
            print "finished starting zookeeper"

        if name == "nimbus":
            time.sleep(5)
            print "starting storm nimbus"
            sh.bash(os.path.join(path, "storm_nimbus.sh"), mapping["zookeeper"])
            print "finished starting storm nimbus"

        if name == "namenode":
            time.sleep(5)
            print "starting yarn namenode"
            sh.bash(os.path.join(path, "yarn_namenode.sh"), mapping["namenode"])
            print "finished starting yarn namenode"

        if name == "resourcemanager":
            time.sleep(5)
            print "starting yarn resourcemanager"
            sh.bash(os.path.join(path, "yarn_resourcemanager.sh"), mapping["namenode"])
            print "finished starting yarn resourcemanager"

        if name == "logging":
            print "starting logging engine"
            sh.bash(os.path.join(path, "logging.sh"))
            print "finished starting logging engine"

        if name == "kafka":
            time.sleep(20)
            print "starting kafka broker"
            sh.bash(os.path.join(path, "kafka_broker.sh"), mapping["zookeeper"], v_node_num)
            print "finished starting kafka broker"

        if name == "yarn":
            time.sleep(20)
            print "starting yarn nodemanager"
            sh.bash(os.path.join(path, "yarn_nodemanager.sh"), mapping["namenode"], mapping["resourcemanager"],
                    v_node_num)
            print "finished starting yarn nodemanager"

        if name == "supervisor":
            time.sleep(20)
            print "starting storm supervisor"
            sh.bash(os.path.join(path, "storm_supervisor.sh"), mapping["zookeeper"], mapping["nimbus"], v_node_num)
            print "finished starting storm supervisor"


def wait(resource_file, node_num, v_node_num, node_file, exec_path):
    """

    :param resource_file:
    :param node_num:
    :param v_node_num:
    :param node_file:
    :return:
    """
    time.sleep(200)


def _read_resource_file(resource_file):
    """
    Reads the resources defined in the resource file into a dictionary.
    :param resource_file:
    :return: dictionary of resource file content ["system"] = "node_name"
    """
    with open(resource_file, mode='r') as f:
        res = json.load(f, object_pairs_hook=collections.OrderedDict)
        return res


def _read_node_service_mapping(resources, node_file):
    """
    Reads the mapping of nodes to services, in order to determine the host a certain service is running on.
    :param resources:
    :param node_file:
    :return:
    """
    node_list = list()
    with open(node_file, mode='r') as f:
        nodes = f.read().splitlines()
        for n in nodes:
            if n not in node_list:
                node_list.append(n)

    system_mapping = dict()
    for node, systems in resources["node"].iteritems():
        n = int(node)
        node_name = node_list[n]
        for s in systems.iterkeys():
            system_mapping[s] = node_name

    return system_mapping


if __name__ == "__main__":
    arguments = docopt(__doc__)

    if arguments['qsub']:
        qsub(arguments['<resource_file>'], arguments['<exec_path>'])

    if arguments['configure_node']:
        configure_node(arguments['<resource_file>'], arguments['<nodenum>'], arguments['<vnodenum>'],
                       arguments['<node_file>'], arguments['<exec_path>'])

    if arguments['wait']:
        wait(arguments['<resource_file>'], arguments['<nodenum>'], arguments['<vnodenum>'], arguments['<node_file>'],
             arguments['<exec_path>'])