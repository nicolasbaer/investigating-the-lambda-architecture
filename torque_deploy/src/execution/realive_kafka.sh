#!/bin/bash

cd ~/lambda/execution


. ./global.sh
. ./install.sh "jre"


# start kafka
cd $job_home/kafka/home
PATH=$PATH JAVA_HOME=$JAVA_HOME nohup bin/kafka-server-start.sh config/server.properties >> $job_home/kafka/logs/kafka_realive.log 2>&1 &
kafka_pid=$!
echo $kafka_pid > $job_home/kafka/pids/pidfile

exit 0