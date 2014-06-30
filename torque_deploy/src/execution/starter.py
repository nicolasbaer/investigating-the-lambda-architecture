#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Torque starter script for `Investigating the lambda architecture`.

Usage:
  starter.py qsub <resource_file> <exec_path>
  starter.py configure_node <resource_file> <nodenum> <vnodenum> <node_file> <exec_path>
  starter.py wait <resource_file> <nodenum> <vnodenum> <node_file> <exec_path> <install_path>
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
import requests
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
            l="nodes=%s:ppn=%s" % (nodes, max_cpus),
            l="walltime=%s" % walltime,
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
        start_service(name, mapping, path, v_node_num)


def start_service(service, mapping, path, v_node_num, sleep=True):
    if service == "coordinator":
        print "starting coordinator"
        sh.bash("coordinator.sh")
        print "finished starting coordinator"

    if service == "zookeeper":
        print "starting zookeeper"
        sh.bash("zookeeper.sh")
        print "finished starting zookeeper"

    if service == "nimbus":
        if sleep:
            time.sleep(5)
        print "starting storm nimbus"
        sh.bash(os.path.join(path, "storm_nimbus.sh"), mapping["zookeeper"])
        print "finished starting storm nimbus"

    if service == "namenode":
        if sleep:
            time.sleep(5)
        print "starting yarn namenode"
        sh.bash(os.path.join(path, "yarn_namenode.sh"), mapping["namenode"])
        print "finished starting yarn namenode"

    if service == "resourcemanager":
        if sleep:
            time.sleep(5)
        print "starting yarn resourcemanager"
        sh.bash(os.path.join(path, "yarn_resourcemanager.sh"), mapping["namenode"])
        print "finished starting yarn resourcemanager"

    if service == "logging":
        print "starting logging engine"
        sh.bash(os.path.join(path, "logging.sh"))
        print "finished starting logging engine"

    if service == "kafka":
        if sleep:
            time.sleep(20)
        print "starting kafka broker"
        sh.bash(os.path.join(path, "kafka_broker.sh"), mapping["zookeeper"], v_node_num)
        print "finished starting kafka broker"

    if service == "yarn":
        if sleep:
            time.sleep(20)
        print "starting yarn nodemanager"
        sh.bash(os.path.join(path, "yarn_nodemanager.sh"), mapping["namenode"], mapping["resourcemanager"],
                v_node_num)
        print "finished starting yarn nodemanager"

    if service == "supervisor":
        if sleep:
            time.sleep(20)
        print "starting storm supervisor"
        sh.bash(os.path.join(path, "storm_supervisor.sh"), mapping["zookeeper"], mapping["nimbus"], v_node_num)
        print "finished starting storm supervisor"

    if service == "mongodb":
        if sleep:
            time.sleep(20)
        print "starting mongodb"
        sh.bash(os.path.join(path, "mongodb.sh"))
        print "finished starting mongodb"

    if service == "redis":
        if sleep:
            time.sleep(20)
        print "starting redis"
        sh.bash(os.path.join(path, "redis.sh"))
        print "finished starting redis"


def kill_service(service, install_root):
    pidfile = os.path.join(install_root, service, "pids", "pidfile")
    with open(pidfile) as f:
        pids = f.read().splitlines()
        for pid in pids:
            sh.kill('-9', pid)

    os.remove(pidfile)


def wait(resource_file, node_num, v_node_num, node_file, exec_path, install_path):
    """

    :param resource_file:
    :param node_num:
    :param v_node_num:
    :param node_file:
    :return:
    """
    res = _read_resource_file(resource_file)
    mapping = _read_node_service_mapping(res, node_file)
    path = os.path.expandvars(os.path.expanduser(exec_path))
    install_path = os.path.expandvars(os.path.expanduser(install_path))

    coordinator_url = "http://%s.ifi.uzh.ch:5000" % mapping['coordinator']

    # register services for current node
    services = res["node"][node_num]
    counter = 0
    ids = dict()
    status = dict()
    for service in services.iterkeys():
        service_id = "%s%s" % (node_num, counter)
        ids[service] = service_id
        status[service] = "running"
        counter += 1

        url = "%s/register/%s.%s.%s.%s" % (coordinator_url, node_num, mapping[service], service, service_id)  # <node_num>.<host>.<service>.<uuid>
        requests.get(url)

    run = True
    while run:
        # check for changes in the service status
        for service, id in ids.iteritems():
            url = "%s/status/%s" % (coordinator_url, id)
            response = requests.get(url)
            state = response.text

            if service == "coordinator" and state != "running":
                run = False
                break

            if state != status[service]:
                if state == "pause":
                    # kill the service
                    kill_service(service, install_path)

                if state == "running":
                    # start the service
                    start_service(service, mapping, path, v_node_num, sleep=False)

                status[service] = state

        time.sleep(10)


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
        print "started!"
        wait(arguments['<resource_file>'], arguments['<nodenum>'], arguments['<vnodenum>'], arguments['<node_file>'],
             arguments['<exec_path>'], arguments['<install_path>'])