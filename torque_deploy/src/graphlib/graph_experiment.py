#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script is responsible to cellect key performance indicators for an experiment

Usage:
  collect_kpis.py <data_path>
  collect_kpis.py -h | --help

Options:
  -h --help     Show this screen.

"""
__author__ = 'Nicolas Bär <nicolas.baer@uzh.ch'

import os
import time

from docopt import docopt
import prettyplotlib as ppl
import matplotlib.pyplot as plt
import matplotlib as mpl
from prettyplotlib import brewer2mpl





def create_throughtput_graph(data_path):
     # create throughput graph
    throughput_file = os.path.join(data_path, "kpi_throughput_health_histogram.csv")
    f = open(throughput_file, "r")

    x = list()
    y1 = list()
    y2 = list()

    for line in f:
        splitted = line.split(",")
        time = splitted[0]
        throughput = splitted[1]
        health = splitted[2]

        x.append(time)
        y1.append(throughput)
        y2.append(health)


    fig, ax1 = plt.subplots()

    ax2 = ax1.twinx()
    ppl.plot(ax1, x, y1, label="throughput", linewidth=0.75, linestyle="dashed")
    ppl.plot(ax2, x, y2, label="health", linewidth=0.75, linestyle="solid", color="g")
    #ax1.plot(x, y1, 'g-')
    #ax2.plot(x, y2, 'b-')

    ax1.set_xlabel('time (s)')
    ax1.set_ylabel('throughput (events/s)', color='b')
    ax2.set_ylabel('health (ts/s)', color='g')

    ax1.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=2, borderaxespad=0.)
    ax2.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=1, borderaxespad=0.)
    #ppl.legend(ax2)

    fig.savefig(os.path.join(data_path, "kpi_throughput_health_histogram.pdf"), format="pdf")


def create_time_window_graph(data_path):
    diagram = os.path.join(data_path, "kpi_time_window_diagram.csv")
    f = open(diagram, "r")

    x = list()
    y_precision = list()
    y_recall = list()
    y_throughput = list()
    y_health = list()

    for l in f:
        splitted = l.split(",")
        window = splitted[0]
        precision = splitted[1]
        recall = splitted[2]
        throughput = splitted[3]
        health = splitted[4]

        x.append(int(window))
        y_precision.append(precision)
        y_recall.append(recall)
        y_throughput.append(throughput)
        y_health.append(health)


    fig, ax = plt.subplots()
    ax2 = ax.twinx()


    ppl.plot(ax, x, y_precision, label="precision", linestyle="solid", color="y")
    ppl.plot(ax, x, y_recall, label="recall", linewidth=0.75, linestyle="dashdot", color="r")
    ppl.plot(ax, x, y_health, label="health", linewidth=0.75, linestyle="dotted", color="g")
    ppl.plot(ax2, x, y_throughput, label="throughput", linewidth=0.75, linestyle="dashed")
    #ax1.plot(x, y1, 'g-')
    #ax2.plot(x, y2, 'b-')

    ax.set_xlabel(u'# time window')
    ax.set_ylabel(u'precision, recall, health')
    ax2.set_ylabel(u'throughput', color='g')

    ax.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=2, borderaxespad=0.)
    ax2.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=1, borderaxespad=0.)
    #ppl.legend(ax2)


    fig.savefig(os.path.join(data_path, "kpi_time_window_diagram.pdf"), format="pdf")


if __name__ == "__main__":
    arguments = docopt(__doc__)

    data_path = os.path.expandvars(os.path.expanduser(arguments['<data_path>']))

    create_throughtput_graph(data_path)
    create_time_window_graph(data_path)






