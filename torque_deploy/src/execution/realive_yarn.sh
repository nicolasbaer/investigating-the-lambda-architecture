#!/bin/bash

cd ~/lambda/execution

. ./global.sh
. ./install.sh "jre"


# restart yarn
cd $job_home/yarn/home
PATH=$PATH JAVA_HOME=$JAVA_HOME nohup bin/yarn nodemanager >> $job_home/yarn/logs/nodemanager_realive.log &

exit 0