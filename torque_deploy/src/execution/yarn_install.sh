#!/bin/bash

# ensure lambda home
. ./lambda_home.sh
# ensure java
. ./java_install.sh

LYARN_PARENT=$LAMBDA_HOME/yarn
LYARN_HOME=$LYARN_PARENT/home
LYARN_DATA=$LYARN_PARENT/data
LYARN_LOGS=$LYARN_PARENT/logs
LYARN_PID=$LYARN_PARENT/pid
LYARN_TMP=$LYARN_PARENT/tmp

if [ ! -d "$LYARN_HOME" ]; then
    mkdir -p $LYARN_HOME
    cp -r $USER_HOME/yarn/* $LYARN_HOME/
fi

mkdir -p $LYARN_DATA
mkdir -p $LYARN_LOGS
mkdir -p $LYARN_PID


