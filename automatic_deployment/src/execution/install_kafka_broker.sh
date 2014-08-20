#!/bin/bash

# parameters
zookeeper_host=$1
kafka_node_number=$2

# install application and dependencies
. ./install.sh "jre"
. ./install.sh "kafka"


# configuration file manipulation
kafka_config=$LAMBDA_APP_HOME/config/server.properties

kafka_port=9092

cp $lambda_home_conf/kafka.properties $kafka_config
sed -ie "s,\$data_dir,$LAMBDA_APP_DATA," $kafka_config
sed -ie "s,\$broker_id,$kafka_node_number," $kafka_config
sed -ie "s,\$port,$kafka_port," $kafka_config
sed -ie "s,\$zookeeper_host,$zookeeper_host," $kafka_config


# start kafka broker
cd $LAMBDA_APP_HOME
PATH=$PATH JAVA_HOME=$JAVA_HOME nohup bin/kafka-server-start.sh config/server.properties > $LAMBDA_APP_LOGS/kafka.log 2>&1 &
kafka_pid=$!
echo $kafka_pid > $LAMBDA_APP_PIDS/pidfile
