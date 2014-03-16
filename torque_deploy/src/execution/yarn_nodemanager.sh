#!/bin/bash

. ./yarn_install.sh

./replace_var_xml.sh $LAMBDA_CONF/core-site.xml $LYARN_HOME/etc/hadoop/core-site.xml LYARN_NAMENODE_HOST $LYARN_NAMENODE_HOST
./replace_var_xml_same.sh $LYARN_HOME/etc/hadoop/core-site.xml LYARN_TMP $LYARN_TMP
./replace_var_xml.sh $LAMBDA_CONF/yarn-site.xml $LYARN_HOME/etc/hadoop/yarn-site.xml LYARNR_HOST $LYARNR_HOST
./replace_var_xml.sh $LAMBDA_CONF/hdfs-site.datanode.xml $LYARN_HOME/etc/hadoop/hdfs-site.xml LYARN_DATA $LYARN_DATA

cd $LYARN_HOME
JAVA_HOME=$JAVA_HOME nohup bin/hdfs datanode > $LYARN_LOGS/datanode.log 2>&1 &
DN_PID=$!
echo $DN_PID > $LYARN_PID/dn.pid

JAVA_HOME=$JAVA_HOME nohup bin/yarn nodemanager > $LYARN_LOGS/nodemanager.log 2>&1 &
NM_PID=$!
echo $NM_PID > $LYARN_PID/nm.pid