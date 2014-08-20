#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script is responsible to cellect key performance indicators for an experiment

Usage:
  collect_kpis.py <result_path> <shutdown_path> <sys_start_time> <data_start_time> <ticks_per_ms> <parallelism> <question>
  collect_kpis.py -h | --help

Options:
  -h --help     Show this screen.

"""
__author__ = 'Nicolas Bär <nicolas.baer@uzh.ch'

import os
import math

from docopt import docopt
from pymongo import MongoClient, ASCENDING, DESCENDING
import requests


host = "localhost"  # it is assumed a mongodb and elasticsearch instance is on the same host!
db_name = "lambda"
result_col_name = "result"
baseline_col_name = "baseline"
health_col_name = "health"

def create_indexes():
    """
    Create indexes on the baseline and result collection in MongoDB to guarantee lower latency queries! Otherwise it takes ages :)
    """
    client = MongoClient(host)
    db = client[db_name]
    results = db[result_col_name]
    baseline = db[baseline_col_name]
    health = db[health_col_name]

    res = results.find().limit(1)
    if res:
        result = res[0]
        del result["sys_time"]
        del result["_id"]

        indexes = list()
        for key in result.iterkeys():
            indexes.append((key, ASCENDING))
        results.create_index(indexes)
        baseline.create_index(indexes)

    results.create_index("ts_start")
    results.create_index("sys_time")
    results.create_index([("ts_start", ASCENDING), ("sys_time", ASCENDING)])

    baseline.create_index("ts_start")
    baseline.create_index("sys_time")
    baseline.create_index([("ts_start", ASCENDING), ("sys_time", ASCENDING)])

    health.create_index([("start", ASCENDING), ("stop", ASCENDING)])


def get_throughput(start, end):
    """
    Calculates the throughput for the given time span in events/s. The calculation is based on the log messages
    in Kibana. The following components are included: (i) Samza processing task (ii) Storm processing bolt

    :param start: start time in ms
    :param end: end time in ms
    :return: throughput in events/s
    """
    interval_time = end - start

    query_path = os.path.join("el_queries", "throughput.json")
    with open(query_path, 'r') as qf:
        query = qf.read()

    query = query.replace("%TIMESTAMP_START", str(start))
    query = query.replace("%TIMESTAMP_END", str(end))

    response = requests.post("http://%s:9200/_all/_count?pretty" % host, data=query)
    data = response.json()

    throughput = data["count"] * 1000 / interval_time
    throughput = throughput * 1000  # throughput in seconds

    return throughput


def get_healthyness(start, end, parallelism):
    """
    Calculates the health of the cluster for a given time period.

    :param start: start time
    :param end: end time
    :param parallelism: number of active machines in total. Only node failures were recorded, so this is essential!
    :return: percentage of available nodes within the given time window from 0 to 1
    """
    client = MongoClient(host)
    db = client[db_name]
    health_db = db[health_col_name]

    query = {"$or":
                 [{"$and":
                       [
                           {"start": {"$gte": start}},
                           {"start": {"$lt": end}}
                       ]
                  },
                  {"$and":
                     [
                         {"stop": {"$gte": start}},
                         {"stop": {"$lt": end}}
                     ]
                  },
                  {"$and":
                     [
                         {"start": {"$lte": start}},
                         {"stop": {"$gte": end}}
                     ]
                  }
                 ]
            }

    total_downtime = 0
    downtimes = health_db.find(query)
    for downtime in downtimes:
        window_start = 0
        window_end = 0
        if downtime["start"] <= start:
            window_start = start
        else:
            window_start = downtime["start"]

        if downtime["stop"] >= end:
            window_end = end
        else:
            window_end = downtime["stop"]

        total_downtime += window_end - window_start

    total_compute_time = (end - start) * (parallelism - 1)
    current_health = 1 - (float(total_downtime) / total_compute_time)

    return current_health


def get_last_timestamp():
    """
    Fetches the last timestamp any processing was recorded to determine the end of an experiment.

    :return: timestamp in ms
    """
    try:
        with open(os.path.join("el_queries", "last_timestamp.json"), "r") as query_file:
            post = query_file.read()

        response = requests.post("http://%s:9200/_all/_search?pretty" % host, data=post)
        data = response.json()

        if "hits" in data:
            if "hits" in data["hits"]:
                if "_source" in data["hits"]["hits"][0]:
                    last = data["hits"]["hits"][0]["_source"]
                    ts = last["timestamp"]
                    return int(ts)

    except Exception as e:
        print "exception found %s" % e.message
        raise


def store_node_failures(shutdown_path, parallelism):
    """
    Reads through the node failure log messages and stores the recorded entries in MongoDB for fast access.

    :param shutdown_path: path the failure log messages are found with the following pattern "fail_<NODENUM>.log"
    :param parallelism: number of machines in the cluster
    """
    client = MongoClient(host)
    db = client[db_name]
    health_db = db[health_col_name]

    # clear health db
    health_db.remove({})

    file_name = "fail_%s.log"

    for i in range(0, parallelism):
        node_file_name = file_name % i
        node_file_path = os.path.join(shutdown_path, node_file_name)

        if os.path.exists(node_file_path):

            with open(node_file_path) as node_file:
                latest_entry = None

                for line in node_file:
                    entry = line.strip().split(",")
                    node_num = entry[0]
                    timestamp = int(entry[1])
                    state = entry[2]

                    if state == "down":
                        store = dict()
                        store['host'] = node_num
                        store['start'] = timestamp * 1000
                        store['desc'] = "downtime"

                        latest_entry = store
                    else:
                        latest_entry['stop'] = timestamp * 1000
                        latest_entry['duration'] = latest_entry['stop'] - latest_entry['start']

                        health_db.insert(latest_entry)

                if "stop" not in latest_entry:
                    latest_entry["stop"] = latest_entry["start"]
                    latest_entry["duration"] = 0
                    health_db.insert(latest_entry)


def create_throughput_health_histogram(start, stop, parallelism, file):
    """
    Creates the data for the throughput/health histogram of the experiment.

    :param start: start time (s)
    :param stop: end time (s)
    :param parallelism: number of machines in the cluster
    :param file: path to store the results
    """
    start_s = start / 1000
    stop_s = stop / 1000
    interval = (stop_s - start_s) / 500
    if interval < 2:
        interval = 2

    ts = start_s

    histogramm = open(file, "w")

    while ts <= stop_s:
        ts_start = ts
        ts_end = interval + ts_start

        throughput = get_throughput(ts_start * 1000, ts_end * 1000)
        health = get_healthyness(ts_start * 1000, ts_end * 1000, parallelism)

        line = "%s,%s,%s\n" % ((ts_end - start_s), throughput, health)
        histogramm.write(line)

        ts += interval

    histogramm.close()


def create_time_window_diagram(start, stop, parallelism, file, file_total, question):
    """
    Creates the data for the time window diagram with the following values for each time window: precision, recall,
    health, throughput, variance, std. deviation.

    :param start: start time (s)
    :param stop: end time (s)
    :param parallelism: number of machines in the cluster
    :param file: file to write the time window data to
    :param file_total: time to write the global statistics to
    """
    diagram = open(file, "w")
    total_out = open(file_total, "w")

    client = MongoClient(host)
    db = client[db_name]
    results = db[result_col_name]
    baseline = db[baseline_col_name]
    health = db[health_col_name]

    # get all time windows
    windows_start = results.find({}).sort("ts_start", ASCENDING).distinct("ts_start")
    time_windows_count = len(windows_start)

    total_precision_entries = 0
    total_variance_entries = 0
    total_std_dev_entries = 0
    total_precision_possible = 0
    total_recall_possible = 0

    for i in range(0, time_windows_count):
        window_start = windows_start[i]
        first_window = results.find({"ts_start": window_start}).sort("sys_time", ASCENDING).limit(1)[0]
        last_window = results.find({"ts_start": window_start}).sort("sys_time", DESCENDING).limit(1)[0]
        first_sys_time = first_window["sys_time"]
        last_sys_time = last_window["sys_time"]
        first_window_time = first_window["ts_start"]
        last_window_time = first_window["ts_end"]

        if i+1 < time_windows_count:
            start_next_window = results.find({"ts_start": windows_start[i+1]}).sort("sys_time", ASCENDING).limit(1)[0]
            if start_next_window["sys_time"] > last_sys_time:
                last_sys_time = start_next_window["sys_time"]

        query = {"ts_start": first_window_time, "ts_end": last_window_time}
        max_entries = results.find(query).count()

        if max_entries == 0 or (last_sys_time - first_sys_time) == 0:
            continue

        precise_entries = 0
        deviation = list()
        for r in results.find(query):
            del r['sys_time']
            del r['_id']

            # first we check if the result is accurate
            p = baseline.find(r).count()
            if p > 0:
                precise_entries += 1
                total_precision_entries += 1
            else:
                # for the debs challenge we can calculate the variance and std dev.
                if "debs" in question:
                    if "predicted_load" in r:
                        key = "predicted_load"
                    else:
                        key = "load"
                    load = r[key]
                    del r[key]

                    p = baseline.find(r)
                    if p and p.count() > 0:
                        diff = load - p[0][key]
                        deviation.append((diff)**2)

            total_precision_possible += 1

            variance_recall = 0
            std_dev_recall = 0
            if deviation:
                variance = sum(deviation) / float(len(deviation))
                std_dev = math.sqrt(variance)
                for d in deviation:
                    if d <= variance:
                        variance_recall += 1
                    if d <= std_dev:
                        std_dev_recall += 1
            else:
                variance = 0
                std_dev = 0

            precision = float(precise_entries) / float(max_entries)
            recall_max = baseline.find(query).count()
            total_recall_possible += recall_max

            if recall_max > 0:
                recall = float(precise_entries) / float(recall_max)
                variance_recall = float(precise_entries + variance_recall) / float(recall_max)
                std_dev_recall = float(precise_entries + std_dev_recall) / float(recall_max)
            else:
                recall = 0

                if last_sys_time - first_sys_time == 0:
                    throughput = 0
                else:
                    throughput = get_throughput(first_sys_time, last_sys_time)

            total_variance_entries = precise_entries + variance_recall
            total_std_dev_entries = precise_entries + std_dev_recall

            health = get_healthyness(first_sys_time, last_sys_time, parallelism)

            line = "%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (i, precision, recall, throughput, health, variance, variance_recall, std_dev, std_dev_recall)
            diagram.write(line)

    # calculate total
    throughput = get_throughput(start, stop)
    health = get_healthyness(start, stop, parallelism)

    precision_total = float(total_precision_entries) / float(total_precision_possible)
    recall_total = float(total_precision_entries) / float(total_recall_possible)
    recall_total_variance = float(total_variance_entries) / float(total_recall_possible)
    recall_total_std_dev = float(total_std_dev_entries) / float(total_recall_possible)

    line = "%s,%s,%s,%s,%s,%s,%s" % (stop - start, precision_total, recall_total, throughput, health, recall_total_variance, recall_total_std_dev)
    total_out.write(line)

    total_out.close()
    diagram.close()

if __name__ == "__main__":
    arguments = docopt(__doc__)

    result_path = os.path.expandvars(os.path.expanduser(arguments['<result_path>']))
    shutdown_path = os.path.expandvars(os.path.expanduser(arguments['<shutdown_path>']))

    ts_start_sys = int(arguments['<sys_start_time>'])
    ts_start_data = int(arguments['<data_start_time>'])
    tickes_per_ms = int(arguments['<ticks_per_ms>'])
    parallelism = int(arguments['<parallelism>'])

    question = arguments['<question>']

    ts_end = int(get_last_timestamp())

    # exclude the first and last 10 seconds from the analysis
    ts_end = ts_end - 10 * 1000
    ts_start = ts_start_sys + 10 * 1000

    interval = (ts_end - ts_start) / 100
    if interval < 2:
        interval = 2

    store_node_failures(shutdown_path, parallelism)

    create_indexes()

    print "range to analyze: %s - %s" % (ts_start, ts_end)

    print "creating histogram"
    f_histogram = os.path.join(result_path, "kpi_throughput_health_histogram.csv")
    create_throughput_health_histogram(ts_start, ts_end, parallelism, f_histogram)

    print "creating timewindow diagram"
    f_time_window = os.path.join(result_path, "kpi_time_window_diagram.csv")
    f_total = os.path.join(result_path, "kpi_total.csv")
    create_time_window_diagram(ts_start, ts_end, parallelism, f_time_window, f_total, question)
