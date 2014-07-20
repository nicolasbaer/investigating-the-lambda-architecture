#!/bin/bash

logging_host=$1

# install application and dependencies
. ./install.sh "jre"
. ./install.sh "collectd"

# configuration options
config=$LAMBDA_APP_HOME/collectd.conf

hostname=$(hostname)
hostname+=".ifi.uzh.ch"

sed -ie "s,\$hostname,$hostname," $config
sed -ie "s,\$logging_host,$logging_host," $config


# start collectd
cd $LAMBDA_APP_HOME/collectd
nohup ~/lambda/collectd/usr/sbin/collectd -f -C $config > $LAMBDA_APP_LOGS/collectd.log 2>&1 &
collectd_pid=$!
echo $collectd_pid > $LAMBDA_APP_PIDS/pidfile
