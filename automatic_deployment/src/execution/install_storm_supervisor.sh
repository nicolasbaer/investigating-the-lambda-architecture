#!/bin/bash

# parameters
zookeeper_host=$1
nimbus_host=$2
storm_node_nr=$3

# install application and dependencies
. ./install.sh "jre"
. ./install.sh "storm"

# configuration file manipulation
storm_config=$LAMBDA_APP_HOME/conf/storm.yaml

storm_port=$((6700 + $storm_node_nr))

cp $lambda_home_conf/storm.yaml $storm_config
sed -ie "s,\$zookeeper_host,$zookeeper_host," $storm_config
sed -ie "s,\$data_dir,$LAMBDA_APP_DATA," $storm_config
sed -ie "s,\$port,$storm_port," $storm_config
sed -ie "s,\$nimbus_host,$nimbus_host," $storm_config

# start storm supervisor
cd $LAMBDA_APP_HOME
PATH=$PATH JAVA_HOME=$JAVA_HOME nohup bin/storm supervisor > $LAMBDA_APP_LOGS/supervisor.log 2>&1 &
supervisor_pid=$!
echo $supervisor_pid > $LAMBDA_APP_PIDS/pidfile
