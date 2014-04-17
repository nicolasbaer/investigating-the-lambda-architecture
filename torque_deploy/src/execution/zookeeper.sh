#!/bin/bash

. ./install.sh "jre"
. ./install.sh "zookeeper"

zookeeper_config=$LAMBDA_APP_HOME/conf/zoo.cfg
zookeeper_port=2181

cp $lambda_home_conf/zookeeper.conf $zookeeper_config
sed -ie "s,\$data_dir,$LAMBDA_APP_DATA," $zookeeper_config
sed -ie "s,\$port,$zookeeper_port," $zookeeper_config

cd $LAMBDA_APP_HOME
JAVA_HOME=$JAVA_HOME bin/zkServer.sh start

# copy pidfile
cp $LAMBDA_APP_DATA/zookeeper_server.pid $LAMBDA_APP_PIDS/pidfile


cd - > /dev/null

