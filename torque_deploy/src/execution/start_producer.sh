#!/bin/bash

# parameters
experiment=$1
dataset=$2
dataset_name=$3
ticks_per_ms=$4
start_time=$5
dataset_start_time=$6

# install application and dependencies
. ./install.sh "jre"

cp -r $lambda_home_run/coordinator/* $coordinator_home/


# coordination paths
jar_path=$coordinator_home/lib/coordination-1.0-SNAPSHOT.jar
kafka_properties_path=$coordinator_home/config/kafka.properties
dataset_path=$job_home/dataset/

if [ $experiment = "speed" ]
then
  PATH=$PATH JAVA_HOME=$JAVA_HOME $JAVA_HOME/bin/java -jar -Xmx7000m $jar_path -dataset $dataset_name -producer netty -ticksPerMs $ticks_per_ms -startDataTime $dataset_start_time -path $dataset_path -startSysTime $start_time
fi

if [ $experiment = "batch-samza" ]
then
  PATH=$PATH JAVA_HOME=$JAVA_HOME $JAVA_HOME/bin/java -jar -Xmx7000m $jar_path -dataset $dataset_name -producer kafka -ticksPerMs $ticks_per_ms -startDataTime $dataset_start_time -kafka-properties $kafka_properties_path -path $dataset_path -topic $dataset_name -startSysTime $start_time
fi

if [ $experiment = "batch-storm" ]
then
  PATH=$PATH JAVA_HOME=$JAVA_HOME $JAVA_HOME/bin/java -jar -Xmx7000m $jar_path -dataset $dataset_name -producer kafka -ticksPerMs $ticks_per_ms -startDataTime $dataset_start_time -kafka-properties $kafka_properties_path -path $dataset_path -topic $dataset_name -startSysTime $start_time
fi