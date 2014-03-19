#!/bin/bash

. ./kafka_install.sh

# configuration file
LKAFKA_CONF_SERVER=$LKAFKA_CONFIG/server.properties
LKAFKA_BROKER_ID=$LKAFKA_NODE_NR
LKAKFA_BROKER_PORT=$((9092 + $LKAFKA_NODE_NR)) # TODO: check port
LKAFKA_BROKER_TMPDIR=$LKAFKA_DATA
LKAFKA_ZOO_ADDR=$LZOO_HOST

cp $LAMBDA_CONF/kafka.properties $LKAFKA_CONF_SERVER
./replace_var_xml_same.sh $LKAFKA_CONF_SERVER LKAFKA_BROKER_ID $LKAFKA_BROKER_ID
./replace_var_xml_same.sh $LKAFKA_CONF_SERVER LKAKFA_BROKER_PORT $LKAKFA_BROKER_PORT
./replace_var_xml_same.sh $LKAFKA_CONF_SERVER LKAFKA_BROKER_TMPDIR $LKAFKA_BROKER_TMPDIR
./replace_var_xml_same.sh $LKAFKA_CONF_SERVER LKAFKA_ZOO_ADDR $LKAFKA_ZOO_ADDR

# start broker
cd $LKAFKA_HOME
JAVA_HOME=$JAVA_HOME nohup bin/kafka-server-start.sh config/server.properties > $LKAFKA_LOGS/kafka.log 2>&1 &