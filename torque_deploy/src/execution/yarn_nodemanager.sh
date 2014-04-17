#!/bin/bash

# parameters
yarn_namenode_host=$1
yarn_resourcemanager_host=$2
yarn_datanode_number=$3

# install application and dependencies
. ./install.sh "jre"
. ./install.sh "yarn"


# configuration file manipulation
yarn_config_hdfs=$LAMBDA_APP_HOME/etc/hadoop/hdfs-site.xml
yarn_config_core=$LAMBDA_APP_HOME/etc/hadoop/core-site.xml
yarn_config_mapred=$LAMBDA_APP_HOME/etc/hadoop/mapred-site.xml
yarn_config_yarn=$LAMBDA_APP_HOME/etc/hadoop/yarn-site.xml

nodemanager_port=$((55100 + $yarn_datanode_number))
nodemanager_web_port=$((55500 + $yarn_datanode_number))

datanode_port=$((56100 + $yarn_datanode_number))
datanode_ipc_port=$((56500 + $yarn_datanode_number))
datanode_web_port=$((57100 + $yarn_datanode_number))


cp $lambda_home_conf/hdfs-site.datanode.xml $yarn_config_hdfs
sed -ie "s,\$data_dir,$LAMBDA_APP_DATA," $yarn_config_hdfs
sed -ie "s,\$datanode_port,$datanode_port," $yarn_config_hdfs
sed -ie "s,\$datanode_ipc_port,$datanode_ipc_port," $yarn_config_hdfs
sed -ie "s,\$datanode_web_port,$datanode_web_port," $yarn_config_hdfs


cp $lambda_home_conf/core-site.xml $yarn_config_core
sed -ie "s,\$tmp_dir,$LAMBDA_APP_TMP," $yarn_config_core
sed -ie "s,\$host,$yarn_namenode_host," $yarn_config_core

cp $lambda_home_conf/yarn-site.xml $yarn_config_yarn
sed -ie "s,\$host,$yarn_resourcemanager_host," $yarn_config_yarn
sed -ie "s,\$nodemanager_port,$nodemanager_port," $yarn_config_yarn
sed -ie "s,\$nodemanager_web_port,$nodemanager_web_port," $yarn_config_yarn

cp $lambda_home_conf/mapred-site.xml $yarn_config_mapred


# start datanode
cd $LAMBDA_APP_HOME
JAVA_HOME=$JAVA_HOME nohup bin/hdfs datanode > $LAMBDA_APP_LOGS/datanode.log 2>&1 &
pid=$!
echo $pid > $LAMBDA_APP_PIDS/pidfile

# start nodemanager
JAVA_HOME=$JAVA_HOME nohup bin/yarn nodemanager > $LAMBDA_APP_LOGS/nodemanager.log 2>&1 &
pid=$!
echo $pid >> $LAMBDA_APP_PIDS/pidfile