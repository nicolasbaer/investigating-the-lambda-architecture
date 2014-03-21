#!/bin/bash

# parameters
yarn_nodename_host=$1

# install application and dependencies
. ./install.sh "jre"
. ./install.sh "yarn"

# configuration file manipulation
yarn_config_core=$LAMBDA_APP_HOME/etc/hadoop/core-site.xml
yarn_config_mapred=$LAMBDA_APP_HOME/etc/hadoop/mapred-site.xml

cp $lambda_home_conf/core-site.xml $yarn_config_core
sed -ie "s,\$tmp_dir,$LAMBDA_APP_TMP," $yarn_config_core
sed -ie "s,\$host,$yarn_nodename_host," $yarn_config_core

cp $lambda_home_conf/mapred-site.xml $yarn_config_mapred


# start resource manager service
cd $LAMBDA_APP_HOME
JAVA_HOME=$JAVA_HOME nohup bin/yarn resourcemanager > $LAMBDA_APP_LOGS/resource_manager.log 2>&1 &
pid=$!
echo $pid > $LAMBDA_APP_PIDS/resource_manager.pid
