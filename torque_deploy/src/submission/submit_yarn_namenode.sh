#!/bin/bash
#PBS -N yarn_nn
#PBS -l nodes=1:ppn=1
#PBS -j oe
#PBS -o yarn_nn.out
#PBS -e yarn_nn.err
#PBS -m n
#PBS -V

cd ~/lambda/execution
chmod +x ./*.sh
source ./yarn_namenode.sh

sleep $SECONDS_SLEEP

cp $LAMBDA_APP_LOGS/namenode.log ~/lambda/logs/