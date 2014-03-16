#!/bin/bash
# start zookeeper


# ensure lambda home
. ./lambda_home.sh
# ensure java
. ./java_install.sh

LZOOKEEPER_PARENT=$LAMBDA_HOME/zookeeper
LZOOKEEPER_HOME=$LZOOKEEPER_PARENT/home
LZOOKEEPER_DATA=$LZOOKEEPER_PARENT/data
LZOOKEEPER_PORT=2181

# copy zookeeper
if [ ! -d "$LZOOKEEPER_HOME" ]; then
    mkdir -p $LZOOKEEPER_PARENT
    cp -r $USER_HOME/zookeeper $LZOOKEEPER_HOME
fi

mkdir -p $LZOOKEEPER_DATA

# check port availability
port_found=false
while [ $port_found = false ]; do
    if [[ -z $(lsof -i:$LZOOKEEPER_PORT) ]]; then
       port_found=true
    else
        LZOOKEEPER_PORT=$(($LZOOKEEPER_PORT + 1))
    fi
done


. ./replace_var.sh $LAMBDA_CONF/zookeeper.conf $LZOOKEEPER_HOME/conf/zoo.cfg


cd $LZOOKEEPER_HOME
. bin/zkServer.sh start
cd - > /dev/null