#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script is responsible to kill a node with a certain proability in a certain time interval.

Usage:
  fail_node.py <node_num> <kill_probability> <interval> <parallelism> <exec_path> <shutdown_path> <log_path> <realive_interval> <node_file> <kill_concurrent_nodes>
  fail_node.py -h | --help

Options:
  -h --help     Show this screen.

"""
__author__ = 'Nicolas Bär <nicolas.baer@uzh.ch'

import os
import random
import subprocess
import time
import requests


from docopt import docopt

last_alive = 0
last_check = 0

def check_shutdown(exec_path, master):
    global last_check
    global last_alive

    if last_check == 0 and last_alive == 0:
        last_check = time.time()
        last_alive = int(round(time.time() * 1000))

        return False

    if time.time() - last_check > 5:
        last_check = time.time()
        try:
            with open(os.path.join(exec_path, "shutdown_query.json"), "r") as f:
                post = f.read()

            response = requests.post("http://%s:9200/_all/_search?pretty" % master, data=post)
            data = response.json()

            if "hits" in data:
                if "hits" in data["hits"]:
                    if "_source" in data["hits"]["hits"][0]:
                        last = data["hits"]["hits"][0]["_source"]
                        ts = last["timestamp"]
                        last_alive = int(ts)

        except Exception as e:
            print "exception found %s" % e.message

    current_ts = int(round(time.time() * 1000))
    if current_ts - last_alive > (1000 * 60 * 5):
        return True
    else:
        return False


def coordinate_node_failure(node_num, kill_probability, interval, exec_path, parallelism, log_path, master,
                            concurrent_node_failures):
    # create directory for each node
    for i in range(1, parallelism):
        path = os.path.join(log_path, str(i))
        if not os.path.exists(path):
            os.makedirs(path)

    last_ts = time.time()
    while not check_shutdown(exec_path, master):
        time.sleep(5)

        if (time.time() - last_ts) < interval + random.randrange(0, 10):
            continue

        last_ts = time.time()
        rand = random.random()
        if rand < kill_probability:

            killed_nodes = set()

            while len(killed_nodes) < concurrent_node_failures:
                node = random.randrange(1, parallelism)
                if node not in killed_nodes:
                    open(os.path.join(log_path, str(node), "down"), 'a').close()
                    killed_nodes.add(node)

            killed_nodes.clear()


def worker_node_failure(node_num, exec_path, log_path, realive_interval, master):

    up = True

    while not check_shutdown(exec_path, master):

        if up:
            path = os.path.join(log_path, str(node_num), "down")
            if os.path.exists(path):
                os.remove(path)
                try:
                    command = os.path.join(exec_path, "fail_node.sh")
                    subprocess.Popen([command])
                except Exception:
                    pass

                with open(os.path.join(log_path, "fail_%s.log" % str(node_num)), "a") as f:
                    f.write("%s,%s,down\n" % (node_num, int(time.time())))

                up = not up
        else:
            try:
                subprocess.Popen([os.path.join(exec_path, "realive_storm.sh")])
                subprocess.Popen([os.path.join(exec_path, "realive_kafka.sh")])
                subprocess.Popen([os.path.join(exec_path, "realive_yarn.sh")])
            except Exception:
                pass

            with open(os.path.join(log_path, "fail_%s.log" % str(node_num)), "a") as f:
                f.write("%s,%s,up\n" % (node_num, int(time.time())))

            up = not up

        if up:
            wait = 1
        else:
            wait = realive_interval

        time.sleep(wait)



if __name__ == "__main__":
    arguments = docopt(__doc__)

    shutdown_path = os.path.expandvars(os.path.expanduser(arguments['<shutdown_path>']))
    exec_path = os.path.expandvars(os.path.expanduser(arguments['<exec_path>']))
    log_path = os.path.expandvars(os.path.expanduser(arguments['<log_path>']))
    node_file_path = os.path.expandvars(os.path.expanduser(arguments['<node_file>']))

    master=""
    with open(node_file_path, "r") as f:
        master = f.readline()
        master = master.strip()
        master = "%s.ifi.uzh.ch" % master

    node_num = int(arguments['<node_num>'])
    parallelism = int(arguments['<parallelism>'])
    kill_probability = float(arguments['<kill_probability>'])
    interval = int(arguments['<interval>'])
    realive_interval = int(arguments['<realive_interval>'])
    concurrent_node_failures = int(arguments['<kill_concurrent_nodes>'])


    # we do not simulate node failures in the master node
    if node_num == 0:
        coordinate_node_failure(node_num, kill_probability, interval, exec_path, parallelism, log_path, master, concurrent_node_failures)
    else:
        worker_node_failure(node_num, exec_path, log_path, realive_interval, master)

    with open(os.path.join(log_path, "shutdown.log"), "a") as f:
        f.write("%s\n" % last_alive)
