#!/bin/bash

. ./global.sh

# coordinator
nohup $lambda_home_pyenv/bin/python $lambda_home_service_monitor/service_monitor.py &
