#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script is responsible to kill a node with a certain proability in a certain time interval.

Usage:
  fail_node.py <node_num> <kill_probability> <interval> <parallelism> <exec_path> <shutdown_path> <log_path>
  fail_node.py -h | --help

Options:
  -h --help     Show this screen.

"""
__author__ = 'Nicolas Bär <nicolas.baer@uzh.ch'

import logging
import os
import random
import subprocess
import time


from docopt import docopt


def check_shutdown(paralellism, shutdown_path):
    if len(os.listdir(shutdown_path)) >= paralellism:
        return True
    else:
        return False


def wait_for_shutdown(parallelism, shutdown_path):
    while not check_shutdown(parallelism, shutdown_path):
        time.sleep(10)

    exit(0)


def fail_node(node_num, kill_probability, interval, exec_path, parallelism, shutdown_path, log_path):

    # we do not simulate node failures in the master node
    if node_num == 0:
        wait_for_shutdown(parallelism, shutdown_path)

    up = True

    while not check_shutdown(parallelism, shutdown_path):

        rand = random.random()

        if rand < kill_probability:
            if up:
                try:
                    command = os.path.join(exec_path, "fail_node.sh")
                    subprocess.Popen([command])
                except Exception as e:
                    pass

                with open(os.path.join(log_path, str(node_num)), "a") as f:
                    f.write("%s,%s,down\n" % (node_num, int(time.time())))
            else:
                try:
                    subprocess.Popen([os.path.join(exec_path, "realive_storm.sh")])
                    subprocess.Popen([os.path.join(exec_path, "realive_kafka.sh")])
                    subprocess.Popen([os.path.join(exec_path, "realive_yarn.sh")])
                except Exception as e:
                    pass

                with open(os.path.join(log_path, str(node_num)), "a") as f:
                    f.write("%s,%s,up\n" % (node_num, int(time.time())))

            up = not up

        time.sleep(interval)

if __name__ == "__main__":
    arguments = docopt(__doc__)

    print arguments

    shutdown_path = os.path.expandvars(os.path.expanduser(arguments['<shutdown_path>']))
    exec_path = os.path.expandvars(os.path.expanduser(arguments['<exec_path>']))
    log_path = os.path.expandvars(os.path.expanduser(arguments['<log_path>']))

    node_num = int(arguments['<node_num>'])
    parallelism = int(arguments['<parallelism>'])
    kill_prbability = float(arguments['<kill_probability>'])
    interval = int(arguments['<interval>'])

    fail_node(node_num, kill_prbability, interval, exec_path, parallelism, shutdown_path, log_path)
