#!/bin/bash

# parameters
question=$1

# install application and dependencies
. ./install.sh "jre"

# samza executable path
samza_home=$lambda_home_run/batch

# run samza question task
PATH=$PATH JAVA_HOME=$JAVA_HOME nohup $samza_home/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=$samza_home/config/$question.properties &
sleep 10

# run samza result task
PATH=$PATH JAVA_HOME=$JAVA_HOME nohup $samza_home/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=$samza_home/config/results.properties &
sleep 10