#!/bin/bash

. ./global.sh

# coordinator
nohup $lambda_home_pyenv/bin/python $lambda_home_coordinator/coordinator.py > coordinator.log 2>&1 &
