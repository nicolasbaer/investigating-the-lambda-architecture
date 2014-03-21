#!/bin/bash

# install application and dependencies
. ./install.sh "jre"
. ./install.sh "yarn"


# configuration file manipulation
yarn_config_hdfs=$LAMBDA_APP_HOME/etc/hadoop/hdfs-site.xml
yarn_config_core=$LAMBDA_APP_HOME/etc/hadoop/core-site.xml
yarn_config_mapred=$LAMBDA_APP_HOME/etc/hadoop/mapred-site.xml

yarn_host=$(host $(cat $PBS_NODEFILE) | grep -Eo '[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}')


cp $lambda_home_conf/hdfs-site.namenode.xml $yarn_config_hdfs
sed -ie "s,\$data_dir,$LAMBDA_APP_DATA," $yarn_config_hdfs

cp $lambda_home_conf/core-site.xml $yarn_config_core
sed -ie "s,\$tmp_dir,$LAMBDA_APP_TMP," $yarn_config_core
sed -ie "s,\$host,$yarn_host," $yarn_config_core

cp $lambda_home_conf/mapred-site.xml $yarn_config_mapred



# format hdfs filesystem
cd $LAMBDA_APP_HOME
JAVA_HOME=$JAVA_HOME ./bin/hdfs namenode -format lambda_cluster > $LAMBDA_APP_LOGS/namenode_format.log

# start namenode service
JAVA_HOME=$JAVA_HOME nohup sbin/hadoop-daemon.sh --config $LAMBDA_APP_HOME/etc/hadoop --script hdfs start namenode > $LAMBDA_APP_LOGS/namenode.log 2>&1 &
NN_PID=$!
echo $NN_PID > $LAMBDA_APP_PIDS/namenode.pid
