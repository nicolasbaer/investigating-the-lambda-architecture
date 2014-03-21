#!/bin/bash
#PBS -N yarn_rm
#PBS -l nodes=1:ppn=1
#PBS -j oe
#PBS -o yarn_rm.out
#PBS -e yarn_rm.err
#PBS -m n
#PBS -V



cd ~/lambda/execution
chmod +x ./*.sh
source ./yarn_resourcemanager.sh $LYARN_NAMENODE_HOST

sleep $SECONDS_SLEEP

cp $LAMBDA_APP_HOME/etc/hadoop/core-site.xml ~/lambda/logs/
cp $LAMBDA_APP_HOME/resourcemanager.log ~/lambda/logs/
