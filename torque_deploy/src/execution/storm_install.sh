#!/bin/bash

# install custom jre

. ./lambda_home.sh
. ./java_install.sh


LSTORM_PARENT=$LAMBDA_HOME/kafka
LSTORM_HOME=$LKAFKA_PARENT/home
LSTORM_DATA=$LKAFKA_PARENT/data
LSTORM_CONFIG=$LKAFKA_HOME/conf
LSTORM_LOGS=$LKAFKA_PARENT/logs

if [ ! -d "$LKAFKA_HOME" ]; then
    mkdir -p $LSTORM_HOME
    cp -r $USER_HOME/storm/* $LSTORM_HOME/
fi

mkdir -p $LSTORM_DATA
mkdir -p $LSTORM_LOGS

