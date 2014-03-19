#!/bin/bash
#PBS -N yarn_data
#PBS -l nodes=1:ppn=1
#PBS -j oe
#PBS -o yarn_data.out
#PBS -e yarn_data.err
#PBS -m n
#PBS -V

cd /home/user/baer/lambda/src/execution
chmod +x ./*.sh
LYARN_NAMENODE_HOST=$LYARN_NAMENODE_HOST LYARNR_HOST=$LYARNR_HOST source ./yarn_nodemanager.sh

sleep 120

mkdir -p /home/user/baer/lambda/logs/$PBS_JOBID/

cp $LYARN_LOGS/* /home/user/baer/lambda/logs/$PBS_JOBID/
mkdir -p /home/user/baer/lambda/logs/$PBS_JOBID/conf
cp $LYARN_HOME/etc/hadoop/* /home/user/baer/lambda/logs/$PBS_JOBID/conf/