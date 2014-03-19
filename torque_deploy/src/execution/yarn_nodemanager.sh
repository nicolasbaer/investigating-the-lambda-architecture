#!/bin/bash

. ./yarn_install.sh


LYARN_NODEMANAGER_PORT=$((55100 + $LYARN_DATA_NR))
LYARN_NODEMANAGER_WEB_PORT=$((55500 + $LYARN_DATA_NR))

LYARN_DNODE_PORT=$((56100 + $LYARN_DATA_NR))
LYARN_DNODE_IPC_PORT=$((56500 + $LYARN_DATA_NR))
LYARN_DNODE_HTTP_PORT=$((57100 + $LYARN_DATA_NR))



./replace_var_xml.sh $LAMBDA_CONF/core-site.xml $LYARN_HOME/etc/hadoop/core-site.xml LYARN_NAMENODE_HOST $LYARN_NAMENODE_HOST
./replace_var_xml_same.sh $LYARN_HOME/etc/hadoop/core-site.xml LYARN_TMP $LYARN_TMP

./replace_var_xml.sh $LAMBDA_CONF/yarn-site.xml $LYARN_HOME/etc/hadoop/yarn-site.xml LYARNR_HOST $LYARNR_HOST
./replace_var_xml_same.sh $LYARN_HOME/etc/hadoop/yarn-site.xml LYARN_NODEMANAGER_PORT $LYARN_NODEMANAGER_PORT
./replace_var_xml_same.sh $LYARN_HOME/etc/hadoop/yarn-site.xml LYARN_NODEMANAGER_WEB_PORT $LYARN_NODEMANAGER_WEB_PORT

./replace_var_xml.sh $LAMBDA_CONF/hdfs-site.datanode.xml $LYARN_HOME/etc/hadoop/hdfs-site.xml LYARN_DATA $LYARN_DATA
./replace_var_xml_same.sh $LYARN_HOME/etc/hadoop/hdfs-site.xml LYARN_DNODE_PORT $LYARN_DNODE_PORT
./replace_var_xml_same.sh $LYARN_HOME/etc/hadoop/hdfs-site.xml LYARN_DNODE_IPC_PORT $LYARN_DNODE_IPC_PORT
./replace_var_xml_same.sh $LYARN_HOME/etc/hadoop/hdfs-site.xml LYARN_DNODE_HTTP_PORT $LYARN_DNODE_HTTP_PORT

cp $LAMBDA_CONF/mapred-site.xml $LYARN_HOME/etc/hadoop/mapred-site.xml


cd $LYARN_HOME
JAVA_HOME=$JAVA_HOME nohup bin/hdfs datanode > $LYARN_LOGS/datanode.log 2>&1 &
DN_PID=$!
echo $DN_PID > $LYARN_PID/dn.pid

JAVA_HOME=$JAVA_HOME nohup bin/yarn nodemanager > $LYARN_LOGS/nodemanager.log 2>&1 &
NM_PID=$!
echo $NM_PID > $LYARN_PID/nm.pid