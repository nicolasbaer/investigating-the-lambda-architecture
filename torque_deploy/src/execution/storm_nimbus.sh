#!/bin/bash

# parameters
zookeeper_host=$1

# install application and dependencies
. ./install.sh "jre"
. ./install.sh "storm"

# configuration file manipulation
storm_config=$LAMBDA_APP_HOME/conf/storm.yaml

storm_port=6700
nimbus_host="0.0.0.0"

cp $lambda_home_conf/storm.yaml $storm_config
sed -ie "s,\$zookeeper_host,$zookeeper_host," $storm_config
sed -ie "s,\$data_dir,$LAMBDA_APP_DATA," $storm_config
sed -ie "s,\$port,$storm_port," $storm_config
sed -ie "s,\$nimbus_host,$nimbus_host," $storm_config

# start nimbus
cd $LAMBDA_APP_HOME
JAVA_HOME=$JAVA_HOME nohup bin/storm nimbus > $LAMBDA_APP_LOGS/nimbus.log 2>&1 &
nimbus_pid=$!
echo $nimbus_pid > $LAMBDA_APP_PIDS/pidfile

# start nimbus web ui
nohup bin/storm ui > $LAMBDA_APP_LOGS/nimbus_ui.log 2>&1 &
nimbus_ui_pid=$!
echo $nimbus_ui_pid >> $LAMBDA_APP_PIDS/pidfile