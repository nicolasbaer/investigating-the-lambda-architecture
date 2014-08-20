#!/bin/bash

cd ~/lambda/execution

. ./global.sh
. ./install.sh "jre"


# restart storm
cd $job_home/storm/home
PATH=$PATH JAVA_HOME=$JAVA_HOME bin/storm supervisor


exit 0