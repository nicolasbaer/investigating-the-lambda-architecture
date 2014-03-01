#!/bin/bash -e

# This script is copied from the hello-samza project and is licensed under Apache 2.0.
# A few modifications are in place though :)
# Copyright (c) 2004-2013 Paul R. Holser, Jr.
# https://github.com/linkedin/hello-samza/blob/master/bin/grid

# This $SCRIPT will download, setup, start, and stop servers for Kafka, YARN, and ZooKeeper,
# as well as downloading, building and locally publishing Samza

if [ -z "$JAVA_HOME" ]; then
  echo "JAVA_HOME not set. Exiting."
  exit 1
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=$(dirname $DIR)
DEPLOY_ROOT_DIR=$BASE_DIR/deploy
DEPLOY_DOWNLOAD_DIR=$BASE_DIR/deploy/download
COMMAND=$1
SYSTEM=$2
SCRIPT="grid.sh"

DOWNLOAD_KAFKA=http://mirrors.sonic.net/apache/kafka/0.8.0/kafka_2.8.0-0.8.0.tar.gz
DOWNLOAD_YARN=http://mirrors.sonic.net/apache/hadoop/common/hadoop-2.2.0/hadoop-2.2.0.tar.gz
DOWNLOAD_ZOOKEEPER=http://archive.apache.org/dist/zookeeper/zookeeper-3.4.5/zookeeper-3.4.5.tar.gz
DOWNLOAD_STORM=http://mirror.switch.ch/mirror/apache/dist/incubator/storm/apache-storm-0.9.1-incubating/apache-storm-0.9.1-incubating.tar.gz

bootstrap() {
  echo "Bootstrapping the system..."
  stop_all
  rm -rf $DEPLOY_ROOT_DIR
  install_all
  start_all
  exit 0
}

install_all() {
  $DIR/$SCRIPT install samza
  $DIR/$SCRIPT install zookeeper
  $DIR/$SCRIPT install yarn
  $DIR/$SCRIPT install kafka
  $DIR/$SCRIPT install storm
}

install_zookeeper() {
  install "zookeeper-3.4.5"
  cp $DEPLOY_ROOT_DIR/$SYSTEM/conf/zoo_sample.cfg $DEPLOY_ROOT_DIR/$SYSTEM/conf/zoo.cfg
}

install_yarn() {
  install "hadoop-2.2.0"
  cp $BASE_DIR/conf/yarn-site.xml $DEPLOY_ROOT_DIR/$SYSTEM/etc/hadoop/yarn-site.xml
}

install_kafka() {
  install "kafka_2.8.0-0.8.0"
  # have to use SIGTERM since nohup on appears to ignore SIGINT
  # and Kafka switched to SIGINT in KAFKA-1031.
  sed -i.bak 's/SIGINT/SIGTERM/g' $DEPLOY_ROOT_DIR/kafka/bin/kafka-server-stop.sh
}

install_samza() {
  mkdir -p $DEPLOY_DOWNLOAD_DIR
  cd $DEPLOY_DOWNLOAD_DIR 
  git clone git://git.apache.org/incubator-samza.git
  cd incubator-samza
  ./gradlew -PscalaVersion=2.8.1 publishToMavenLocal
  cd ../..
}

install_storm() {
  install "apache-storm-0.9.1-incubating"
  cp $BASE_DIR/conf/storm.yaml $DEPLOY_ROOT_DIR/$SYSTEM/conf/storm.yaml
}

install() {
  PACKAGE_DIR=$1
  mkdir -p $DEPLOY_DOWNLOAD_DIR
  rm -rf $DEPLOY_ROOT_DIR/$SYSTEM
  DOWNLOAD_COMMAND="curl \$DOWNLOAD_""`echo $SYSTEM|tr '[a-z]' '[A-Z]'`"
  eval $DOWNLOAD_COMMAND > $DEPLOY_DOWNLOAD_DIR/$SYSTEM.tar.gz
  tar -xf $DEPLOY_DOWNLOAD_DIR/$SYSTEM.tar.gz -C $DEPLOY_DOWNLOAD_DIR
  mv $DEPLOY_DOWNLOAD_DIR/$PACKAGE_DIR $DEPLOY_ROOT_DIR/$SYSTEM
}

start_all() {
  $DIR/$SCRIPT start zookeeper
  $DIR/$SCRIPT start yarn
  $DIR/$SCRIPT start kafka
  $DIR/$SCRIPT start storm
}

start_zookeeper() {
  if [ -f $DEPLOY_ROOT_DIR/$SYSTEM/bin/zkServer.sh ]; then
    cd $DEPLOY_ROOT_DIR/$SYSTEM
    bin/zkServer.sh start
    cd - > /dev/null
  else
    echo 'Zookeeper is not installed. Run: bin/$SCRIPT install zookeeper'
  fi
}

start_yarn() {
  if [ -f $DEPLOY_ROOT_DIR/$SYSTEM/bin/yarn ]; then
    cd $DEPLOY_ROOT_DIR/$SYSTEM
    mkdir -p pids logs
    nohup bin/yarn resourcemanager > logs/rm.log 2>&1 &
    RM_PID=$!
    echo $RM_PID > pids/rm.pid
    nohup bin/yarn nodemanager > logs/nm.log 2>&1 &
    NM_PID=$!
    echo $NM_PID > pids/nm.pid
    cd - > /dev/null
  else
    echo 'YARN is not installed. Run: bin/$SCRIPT install yarn'
  fi
}

start_kafka() {
  if [ -f $DEPLOY_ROOT_DIR/$SYSTEM/bin/kafka-server-start.sh ]; then
    mkdir -p $DEPLOY_ROOT_DIR/$SYSTEM/logs
    cd $DEPLOY_ROOT_DIR/$SYSTEM
    nohup bin/kafka-server-start.sh config/server.properties > logs/kafka.log 2>&1 &
    cd - > /dev/null
  else
    echo 'Kafka is not installed. Run: bin/$SCRIPT install kafka'
  fi
}

start_storm() {
  if [ -f $DEPLOY_ROOT_DIR/$SYSTEM/bin/storm ]; then
    cd $DEPLOY_ROOT_DIR/$SYSTEM
    mkdir -p pids logs
    nohup bin/storm nimbus > logs/nimbus.log 2>&1 &
    NIMBUS_PID=$!
    echo $NIMBUS_PID > pids/nimbus.pid
    nohup bin/storm supervisor > logs/supervisor.log 2>&1 &
    SUPERVISOR_PID=$!
    echo $SUPERVISOR_PID > pids/supervisor.pid
    nohup bin/storm ui > logs/ui.log 2>&1 &
    UI_PID=$!
    echo $UI_PID > pids/ui.pid
    echo "Storm UI is available on port 8080"
    cd - > /dev/null
  else
    echo 'Storm is not installed. Run: bin/$SCRIPT install storm'
  fi
}

stop_all() {
  $DIR/$SCRIPT stop kafka
  $DIR/$SCRIPT stop yarn
  $DIR/$SCRIPT stop zookeeper
  $DIR/$SCRIPT stop storm
}

stop_zookeeper() {
  if [ -f $DEPLOY_ROOT_DIR/$SYSTEM/bin/zkServer.sh ]; then
    cd $DEPLOY_ROOT_DIR/$SYSTEM
    bin/zkServer.sh stop
    cd - > /dev/null
  else
    echo 'Zookeeper is not installed. Run: bin/$SCRIPT install zookeeper'
  fi
}

stop_yarn() {
  if [ -f $DEPLOY_ROOT_DIR/$SYSTEM/bin/yarn ]; then
    cd $DEPLOY_ROOT_DIR/$SYSTEM
    RM_PID=`cat pids/rm.pid`
    NM_PID=`cat pids/nm.pid`
    kill $RM_PID
    kill $NM_PID
    rm -rf pids/rm.pid
    rm -rf pids/nm.pid
    cd - > /dev/null
  else
    echo 'YARN is not installed. Run: bin/$SCRIPT install yarn'
  fi
}

stop_kafka() {
  if [ -f $DEPLOY_ROOT_DIR/$SYSTEM/bin/kafka-server-stop.sh ]; then
    cd $DEPLOY_ROOT_DIR/$SYSTEM
    bin/kafka-server-stop.sh
    cd - > /dev/null
  else
    echo 'Kafka is not installed. Run: bin/$SCRIPT install kafka'
  fi
}

stop_yarn() {
  if [ -f $DEPLOY_ROOT_DIR/$SYSTEM/bin/storm ]; then
    cd $DEPLOY_ROOT_DIR/$SYSTEM
    NIMBUS_PID=`cat pids/nimbus.pid`
    SUPERVISOR_PID=`cat pids/supervisor.pid`
    UI_PID=`cat pids/ui.pid`
    kill $NIMBUS_PID
    kill $SUPERVISOR_PID
    kill $UI_PID
    rm -rf pids/nimbus.pid
    rm -rf pids/supervisor.pid
    rm -rf pids/ui.pid
    cd - > /dev/null
  else
    echo 'Storm is not installed. Run: bin/$SCRIPT install storm'
  fi
}

# Check arguments
if [ "$COMMAND" == "bootstrap" ] && test -z "$SYSTEM"; then
  bootstrap
  exit 0
elif (test -z "$COMMAND" && test -z "$SYSTEM") \
  || ( [ "$COMMAND" == "help" ] || test -z "$COMMAND" || test -z "$SYSTEM"); then
  echo
  echo "  Usage.."
  echo
  echo "  $ grid.sh"
  echo "  $ grid.sh bootstrap"
  echo "  $ grid.sh install [yarn|kafka|zookeeper|storm|all]"
  echo "  $ grid.sh start [yarn|kafka|zookeeper|storm|all]"
  echo "  $ grid.sh stop [yarn|kafka|zookeeper|storm|all]"
  echo
  exit 1
else
  echo "EXECUTING: $COMMAND $SYSTEM"
  
  "$COMMAND"_"$SYSTEM"
fi