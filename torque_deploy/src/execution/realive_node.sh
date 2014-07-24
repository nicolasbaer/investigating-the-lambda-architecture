#!/bin/bash

log=~/f_log/realive-$SLURM_NODEID.log

echo "cd to execution" >> $log
cd ~/lambda/execution

echo "import global" >> $log
. ./global.sh

echo "install jre" >> $log
. ./install.sh "jre"


# restart storm
echo "cd storm" >> $log
cd $job_home/storm/home
echo "start storm" >> $log
PATH=$PATH JAVA_HOME=$JAVA_HOME bin/storm supervisor
echo "started storm with pid: $storm_realive_pid"

# restart kafka
echo "cd kafka" >> $log
cd $job_home/kafka/home
echo "start kafka" >> $log
PATH=$PATH JAVA_HOME=$JAVA_HOME nohup bin/kafka-server-start.sh config/server.properties >> $job_home/kafka/logs/kafka_realive.log 2>&1 &
kafka_pid=$!
echo $kafka_pid > $job_home/kafka/pids/pidfile

# restart yarn
echo "cd yarn" >> $log
cd $job_home/yarn/home
echo "start yarn" >> $log
PATH=$PATH JAVA_HOME=$JAVA_HOME nohup bin/yarn nodemanager >> $job_home/yarn/logs/nodemanager_realive.log &
echo "stopped starting yarn" >> $log

echo "exitting"
exit 0