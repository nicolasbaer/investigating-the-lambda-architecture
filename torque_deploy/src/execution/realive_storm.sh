#!/bin/bash

log=~/f_log/realive-$SLURM_NODEID.log

echo "cd to execution" >> $log
cd ~/lambda/execution

echo "import global" >> $log
. ./global.sh

echo "install jre" >> $log
. ./install.sh "jre"


# restart storm
echo "cd storm" >> $log
cd $job_home/storm/home
echo "start storm" >> $log
PATH=$PATH JAVA_HOME=$JAVA_HOME bin/storm supervisor
echo "started storm with pid: $storm_realive_pid"


exit 0