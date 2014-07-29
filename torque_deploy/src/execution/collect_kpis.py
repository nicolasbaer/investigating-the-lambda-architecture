#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script is responsible to cellect key performance indicators for an experiment

Usage:
  collect_kpis.py <result_path> <shutdown_path> <sys_start_time> <data_start_time> <ticks_per_ms> <parallelism>
  collect_kpis.py -h | --help

Options:
  -h --help     Show this screen.

"""
__author__ = 'Nicolas Bär <nicolas.baer@uzh.ch'

import os
import time
import json

from docopt import docopt
from pymongo import MongoClient, ASCENDING
import requests

host = "localhost"
db_name = "lambda"
result_col_name = "result"
baseline_col_name = "baseline"
health_col_name = "health"

def create_indexes():
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

    health.create_index([("start", ASCENDING), ("stop", ASCENDING)])


def get_precision_recall(start, end):
    client = MongoClient(host)
    db = client[db_name]
    results = db[result_col_name]
    baseline = db[baseline_col_name]

    query = {"$and": [{"sys_time": {"$gt": start}}, {"sys_time": {"$lte": end}}]}

    max_entries = results.find(query).count()

    if max_entries == 0:
        return "-", "-"

    precise_entries = 0

    first = None
    last = None

    for r in results.find(query):
        #if r["sys_time"] < start or r["sys_time"] > end:
        #    print "wrong data!"

        del r['sys_time']
        del r['_id']

        p = baseline.find(r).count()

        if p > 0:
            precise_entries += 1

    # query_recall = {"$and": [{"ts_start": {"$gte": earliest}}, {"sys_time": {"$lte": latest}}]}
    # max_results = baseline.find(query_recall).count()

    # if max_results <= 0:
    #    recall = "-"
    #else:
    #    recall = (precise_entries / max_results)

    recall = "-"
    precision = float(precise_entries) / float(max_entries)

    return precision, recall


def get_throughput(start, end):

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
    client = MongoClient(host)
    db = client[db_name]
    health_db = db[health_col_name]

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



if __name__ == "__main__":
    arguments = docopt(__doc__)

    result_path = os.path.expandvars(os.path.expanduser(arguments['<result_path>']))
    shutdown_path = os.path.expandvars(os.path.expanduser(arguments['<shutdown_path>']))

    ts_start_sys = int(arguments['<sys_start_time>'])
    ts_start_data = int(arguments['<data_start_time>'])
    tickes_per_ms = int(arguments['<ticks_per_ms>'])
    parallelism = int(arguments['<parallelism>'])

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





    # calculate histogram data
    f = open(os.path.join(result_path, "kpi_histogram.csv"), "w")

    start = ts_start
    while start < ts_end:
        end = start + interval

        if end > ts_end:
            end = ts_end

        precision, recall = get_precision_recall(start, end)
        throughput = get_throughput(start, end)
        healthyness = get_healthyness(start, end, parallelism)

        ts = end - ts_start

        f.write("%s,%s,%s,%s,%s\n" % (ts / 1000, precision, recall, throughput, healthyness))
        print "%s,%s,%s,%s,%s\n" % (ts / 1000, precision, recall, throughput, healthyness)

        start = start + interval

    f.close()

    # calculate over-all data
    precision, recall = get_precision_recall(ts_start, ts_end)
    throughput = get_throughput(ts_start, ts_end)
    healthyness = get_healthyness(ts_start, ts_end, parallelism)

    f = open(os.path.join(result_path, "kpi_total.csv"), "w")
    f.write("%s,%s,%s,%s,%s\n" % (ts / 1000, precision, recall, throughput, healthyness))


    f.close()