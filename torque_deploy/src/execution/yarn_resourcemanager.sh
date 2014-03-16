#!/bin/bash

. ./yarn_install.sh

./replace_var_xml.sh $LAMBDA_CONF/core-site.xml $LYARN_HOME/etc/hadoop/core-site.xml LYARN_NAMENODE_HOST $LYARN_NAMENODE_HOST
./replace_var_xml_same.sh $LYARN_HOME/etc/hadoop/core-site.xml LYARN_TMP $LYARN_TMP

cd $LYARN_HOME
JAVA_HOME=$JAVA_HOME nohup bin/yarn resourcemanager > $LYARN_LOGS/rm.log 2>&1 &
RM_PID=$!
echo $RM_PID > $LYARN_PID/rm.pid
