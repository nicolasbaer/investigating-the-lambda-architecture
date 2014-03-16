#!/bin/bash

# install custom jre

. ./lambda_home.sh


JAVA_HOME=/$LAMBDA_HOME/jre

if [ ! -d "$JAVA_HOME" ]; then
    cp -r $USER_HOME/jre $JAVA_HOME
fi



