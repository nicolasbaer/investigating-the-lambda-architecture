#!/bin/bash

. ./install.sh "jre"
. ./install.sh "redis"


cd $LAMBDA_APP_HOME

nohup src/redis-server > $LAMBDA_APP_LOGS/redis.log 2>&1 &
redis_pid=$!
echo $redis_pid > $LAMBDA_APP_PIDS/pidfile

cd - > /dev/null
