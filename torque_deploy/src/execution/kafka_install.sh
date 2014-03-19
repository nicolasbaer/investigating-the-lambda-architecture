#!/bin/bash

# install custom jre

. ./lambda_home.sh
. ./java_install.sh

LKAFKA_PARENT=$LAMBDA_HOME/kafka
LKAFKA_HOME=$LKAFKA_PARENT/home
LKAFKA_DATA=$LKAFKA_PARENT/data
LKAFKA_CONFIG=$LKAFKA_HOME/config
LKAFKA_LOGS=$LKAFKA_PARENT/logs

if [ ! -d "$LKAFKA_HOME" ]; then
    mkdir -p $LKAFKA_HOME
    cp -r $USER_HOME/kafka/* $LKAFKA_HOME/
fi

mkdir -p $LKAFKA_DATA
mkdir -p $LKAFKA_LOGS

