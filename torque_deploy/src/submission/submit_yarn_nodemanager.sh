#!/bin/bash
#PBS -N yarn_data
#PBS -l nodes=1:ppn=1
#PBS -j oe
#PBS -o yarn_data_$LYARN_DATA_NR.out
#PBS -e yarn_data_$LYARN_DATA_NR.err
#PBS -m n
#PBS -V


cd ~/lambda/execution
chmod +x ./*.sh
source ./yarn_nodemanager.sh $LYARN_NAMENODE_HOST $LYARNR_HOST $LYARN_DATA_NR

sleep $SECONDS_SLEEP

mkdir -p ~/lambda/logs/$PBS_JOBID/

cp $LAMBDA_APP_LOGS/* ~/lambda/logs/$PBS_JOBID/
mkdir -p ~/lambda/logs/$PBS_JOBID/conf
cp $LAMBDA_APP_HOME/etc/hadoop/* ~/lambda/logs/$PBS_JOBID/conf/