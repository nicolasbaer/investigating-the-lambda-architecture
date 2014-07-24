#!/bin/bash

cd ~/lambda/execution

. ./global.sh
. ./install.sh "jre"


# restart yarn
cd $job_home/yarn/home
PATH=$PATH JAVA_HOME=$JAVA_HOME nohup bin/yarn nodemanager >> $job_home/yarn/logs/nodemanager_realive.log &

PATH=$PATH:/home/slurm/baer-183181/lambda/jre/bin JAVA_HOME=/home/slurm/baer-183181/lambda/jre nohup bin/yarn nodemanager >> /home/slurm/baer-183181/lambda/yarn/logs/nodemanager_realive.log &


exit 0