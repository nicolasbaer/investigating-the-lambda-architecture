#!/bin/bash

. ./global.sh


# backup logging output
result_logging_path=$experiment_home/logs
mkdir -p $result_logging_path


# stop elasticsearch
kill $(ps aux | grep 'baer' | grep 'elasticsearch' | awk '{print $2}')

# wait for a moment
sleep 5

tar czf $result_logging_path/elasticsearch.tar.gz $job_home/logging/home/elasticsearch/data