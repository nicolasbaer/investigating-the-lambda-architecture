#!/bin/bash

# parameters
dataset_name=$1
question=$2

# install application and dependencies
. ./install.sh "jre"

# storm path
storm_path=$job_home/storm/home/

# speed layer jar path
jar_path=$lambda_home_run/speed/speed-1.0-SNAPSHOT.jar
jar_main="ch.uzh.ddis.thesis.lambda_architecture.speed.topology.TopologyHelper"

# start nodemanager
PATH=$PATH JAVA_HOME=$JAVA_HOME $storm_path/bin/storm jar $jar_path $jar_main -dataset $dataset_name -question $question