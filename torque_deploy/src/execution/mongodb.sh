#!/bin/bash

. ./install.sh "jre"
. ./install.sh "mongodb"


cd $LAMBDA_APP_HOME

nohup bin/mongod --dbpath $LAMBDA_APP_DATA > $LAMBDA_APP_LOGS/mongod.log 2>&1 &
mongodb_pid=$!
echo $mongodb_pid > $LAMBDA_APP_PIDS/pidfile

cd - > /dev/null
