#!/bin/bash

. ./storm_install.sh

# configuration file
LSTORM_CONF_SERVER=$LSTORM_CONFIG/storm.yaml
LSTORM_ZOO_ADDR=$LZOO_HOST

cp $LAMBDA_CONF/storm.yaml $LSTORM_CONF_SERVER
./replace_var_xml_same.sh $LSTORM_CONF_SERVER LSTORM_ZOO_ADDR $LSTORM_ZOO_ADDR

# start broker
cd $LSTORM_HOME
JAVA_HOME=$JAVA_HOME nohup bin/kafka-server-start.sh config/server.properties > $LKAFKA_LOGS/kafka.log 2>&1 &