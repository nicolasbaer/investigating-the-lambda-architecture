#!/bin/bash

# install application and dependencies
. ./install.sh "jre"
. ./install.sh "logging"

# configuration options
kibana_config=$LAMBDA_APP_HOME/jetty/webapps/kibana/config.js

hostname=$(hostname)
hostname+=".ifi.uzh.ch"

cp $lambda_home_conf/kibana.js $kibana_config
sed -ie "s,\$elasticsearch_host,$hostname," $kibana_config


# start elasticsearch
cd $LAMBDA_APP_HOME/elasticsearch
JAVA_HOME=$JAVA_HOME nohup bin/elasticsearch > $LAMBDA_APP_LOGS/elasticsearch.log 2>&1 &
elasticsearch_pid=$!
echo $elasticsearch_pid > $LAMBDA_APP_PIDS/pidfile

# start flume
cd $LAMBDA_APP_HOME/flume
JAVA_HOME=$JAVA_HOME nohup bin/flume-ng agent --conf conf --conf-file conf/example.conf --name a1 -Dflume.root.logger=INFO,console > $LAMBDA_APP_LOGS/flume.log 2>&1 &
flume_pid=$!
echo $flume_pid >> $LAMBDA_APP_PIDS/pidfile

# start kibana
cd $LAMBDA_APP_HOME/jetty
nohup $JAVA_HOME/bin/java -jar -Xmx512m start.jar jetty.port=8081 > $LAMBDA_APP_LOGS/jetty.log 2>&1 &
jetty_pid=$!
echo $jetty_pid >> $LAMBDA_APP_PIDS/pidfile

# start logstash
cd $LAMBDA_APP_HOME/logstash
nohup bin/logstash -e 'input { collectd { } } output { elasticsearch_http { host => "localhost" } }' > $LAMBDA_APP_LOGS/logstash.log 2>&1 &
logstash_pid=$!
echo $logstash_pid >> $LAMBDA_APP_PIDS/pidfile
