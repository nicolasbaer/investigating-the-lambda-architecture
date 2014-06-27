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
DOWNLOAD_CASSANDRA=http://mirror.switch.ch/mirror/apache/dist/cassandra/2.0.8/apache-cassandra-2.0.8-bin.tar.gz
DOWNLOAD_FLUME=http://mirror.switch.ch/mirror/apache/dist/flume/1.4.0/apache-flume-1.4.0-bin.tar.gz
DOWNLOAD_ELASTICSEARCH=https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.0.1.tar.gz
DOWNLOAD_KIBANA=https://download.elasticsearch.org/kibana/kibana/kibana-3.0.0milestone5.tar.gz
DOWNLOAD_JETTY=http://eclipse.mirror.kangaroot.net/jetty/stable-9/dist/jetty-distribution-9.1.3.v20140225.tar.gz
DOWNLOAD_REDIS=http://download.redis.io/releases/redis-2.8.12.tar.gz
# this is osx specific!!
DOWNLOAD_MONGODB=http://fastdl.mongodb.org/linux/mongodb-osx-x86_64-2.6.1.tgz

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
  $DIR/$SCRIPT install cassandra
  $DIR/$SCRIPT install mongodb
  $DIR/$SCRIPT install redis
  $DIR/$SCRIPT install logging
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

install_cassandra() {
  install "apache-cassandra-2.0.8"
  cp $BASE_DIR/conf/cassandra.yaml $DEPLOY_ROOT_DIR/$SYSTEM/conf/cassandra.yaml
}

install_redis() {
  install "redis-2.8.12"
  cd $DEPLOY_ROOT_DIR/$SYSTEM
  make
  cd - > /dev/null
}

install_mongodb() {
  echo "Unfortunately you'll have to install mongodb yourself, since there's no direct download link"
}

install_logging(){
    install "apache-flume-1.4.0-bin" "flume"
    cp $BASE_DIR/conf/flume.conf $DEPLOY_ROOT_DIR/flume/conf/example.conf

    install "elasticsearch-1.0.1" "elasticsearch"
    cp $BASE_DIR/conf/elasticsearch.yml $DEPLOY_ROOT_DIR/elasticsearch/config/elasticsearch.yml

    cp $DEPLOY_ROOT_DIR/elasticsearch/lib/elasticsearch-* $DEPLOY_ROOT_DIR/flume/lib/
    rm $DEPLOY_ROOT_DIR/flume/lib/lucene-core-*
    cp $DEPLOY_ROOT_DIRelasticsearch/lib/lucene-core-* $DEPLOY_ROOT_DIR/flume/lib/
    cp $BASE_DIR/lib/flume-ng-elasticsearch-serializer-num-1.0-SNAPSHOT-jar-with-dependencies.jar $DEPLOY_ROOT_DIR/flume/lib


    install "jetty-distribution-9.1.3.v20140225" "jetty"
    install "kibana-3.0.0milestone5" "kibana"
    mv $DEPLOY_ROOT_DIR/kibana $DEPLOY_ROOT_DIR/jetty/webapps/
    mv $BASE_DIR/conf/kibana.js $DEPLOY_ROOT_DIR/jetty/webapps/kibana/config.js
}

install() {
  PACKAGE_DIR=$1
  if [ "$2" != "" ]; then
    SYSTEM=$2
  fi

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
  $DIR/$SCRIPT start cassandra
  $DIR/$SCRIPT start redis
  $DIR/$SCRIPT start mongodb
  $DIR/$SCRIPT start logging
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

start_cassandra() {
  if [ -f $DEPLOY_ROOT_DIR/$SYSTEM/bin/cassandra ]; then
    cd $DEPLOY_ROOT_DIR/$SYSTEM
    mkdir -p logs
    nohup bin/cassandra > logs/cassandra.log 2>&1 &
    echo "Cassandra started..."
    cd - > /dev/null
  else
    echo 'Cassandra is not installed. Run: bin/$SCRIPT install cassandra'
  fi
}

start_mongodb() {
  if [ -f $DEPLOY_ROOT_DIR/$SYSTEM/bin/mongod ]; then
    cd $DEPLOY_ROOT_DIR/$SYSTEM
    mkdir -p /tmp/mongodb-data
    mkdir -p logs
    mkdir -p pids
    nohup bin/mongod --dbpath /tmp/mongodb-data > logs/mongod.log 2>&1 &
    MONGO_PID=$!
    echo $MONGO_PID > pids/mongodb.pid
    echo "MongoDB started..."
    cd - > /dev/null
  else
    echo 'MongoDB is not installed. Run: bin/$SCRIPT install mongodb'
  fi
}

start_redis() {
  if [ -f $DEPLOY_ROOT_DIR/$SYSTEM/src/redis-server ]; then
    cd $DEPLOY_ROOT_DIR/$SYSTEM
    mkdir -p logs
    mkdir -p pids
    nohup src/redis-server > logs/redis.log 2>&1 &
    REDIS_PID=$!
    echo $REDIS_PID > pids/redis.pid
    echo "Redis started..."
    cd - > /dev/null
  else
    echo 'Redis is not installed. Run: bin/$SCRIPT install redis'
  fi
}

start_logging() {
  if [ -f $DEPLOY_ROOT_DIR/elasticsearch/bin/elasticsearch ]; then
    cd $DEPLOY_ROOT_DIR/elasticsearch
    mkdir -p pids logs
    nohup bin/elasticsearch > logs/elasticsearch.log 2>&1 &
    ELASTICSEARCH_PID=$!
    echo $ELASTICSEARCH_PID > pids/elasticsearch.pid
    cd - > /dev/null
  else
    echo 'Elasticsearch is not installed. Run: bin/$SCRIPT install elasticsearch'
  fi

  if [ -f $DEPLOY_ROOT_DIR/flume/bin/flume-ng ]; then
    cd $DEPLOY_ROOT_DIR/flume
    mkdir -p pids logs
    nohup bin/flume-ng agent --conf conf --conf-file conf/example.conf --name a1 -Dflume.root.logger=INFO,console > logs/flume.log 2>&1 &
    FLUME_PID=$!
    echo $FLUME_PID > pids/flume.pid
    cd - > /dev/null
  else
    echo 'Flume is not installed. Run: bin/$SCRIPT install flume'
  fi

  if [ -f $DEPLOY_ROOT_DIR/jetty/start.jar ]; then
    cd $DEPLOY_ROOT_DIR/jetty
    mkdir -p pids logs
    nohup java -jar start.jar jetty.port=8081 > logs/jetty.log 2>&1 &
    JETTY_PID=$!
    echo $JETTY_PID > pids/jetty.pid
    echo "Kibana is now available on http://localhost:8081/kibana"
    cd - > /dev/null
  else
    echo 'Jetty is not installed. Run: bin/$SCRIPT install jetty'
  fi



}

stop_all() {
  $DIR/$SCRIPT stop kafka
  $DIR/$SCRIPT stop yarn
  $DIR/$SCRIPT stop zookeeper
  $DIR/$SCRIPT stop storm
  $DIR/$SCRIPT stop cassandra
  $DIR/$SCRIPT stop mongodb
  $DIR/$SCRIPT stop redis
  $DIR/$SCRIPT stop logging
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

stop_storm() {
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

stop_mongodb() {
  if [ -f $DEPLOY_ROOT_DIR/$SYSTEM/bin/mongod ]; then
    cd $DEPLOY_ROOT_DIR/$SYSTEM
    MONGOD_PID=`cat pids/mongodb.pid`
    kill $MONGOD_PID
    rm -rf pids/mongodb.pid
    cd - > /dev/null
  else
    echo 'MongoDB is not installed. Run: bin/$SCRIPT install mongodb'
  fi
}

stop_redis() {
  if [ -f $DEPLOY_ROOT_DIR/$SYSTEM/src/redis-server ]; then
    cd $DEPLOY_ROOT_DIR/$SYSTEM
    REDIS_PID=`cat pids/redis.pid`
    kill $REDIS_PID
    rm -rf pids/redis.pid
    cd - > /dev/null
  else
    echo 'Redis is not installed. Run: bin/$SCRIPT install redis'
  fi
}

stop_cassandra() {
  if [ -f $DEPLOY_ROOT_DIR/$SYSTEM/bin/cassandra ]; then
    cd $DEPLOY_ROOT_DIR/$SYSTEM
    pkill -f CassandraDaemon
    cd - > /dev/null
  else
    echo 'Cassandra is not installed. Run: bin/$SCRIPT install cassandra'
  fi
}

stop_logging(){
    cd $DEPLOY_ROOT_DIR
    ELASTICSEARCH_PID=`cat elasticsearch/pids/elasticsearch.pid`
    FLUME_PID=`cat flume/pids/flume.pid`
    JETTY_PID=`cat jetty/pids/jetty.pid`
    kill $ELASTICSEARCH_PID
    kill $FLUME_PID
    kill $JETTY_PID
    rm -rf elasticsearch/pids/elasticsearch.pid
    rm -rf flume/pids/flume.pid
    rm -rf jetty/pids/jetty.pid
    cd - > /dev/null
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
  echo "  $ grid.sh install [yarn|kafka|zookeeper|storm|logging|all]"
  echo "  $ grid.sh start [yarn|kafka|zookeeper|storm|logging|all]"
  echo "  $ grid.sh stop [yarn|kafka|zookeeper|storm|logging|all]"
  echo
  exit 1
else
  echo "EXECUTING: $COMMAND $SYSTEM"
  
  "$COMMAND"_"$SYSTEM"
fi