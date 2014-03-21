#!/bin/bash
#PBS -N logging
#PBS -l nodes=1:ppn=1
#PBS -j oe
#PBS -o logging.out
#PBS -e logging.err
#PBS -m n
#PBS -V

cd ~/lambda/execution
chmod +x ./*.sh
source ./logging.sh

sleep $SECONDS_SLEEP

mkdir -p ~/lambda/logs/logging
cp $LAMBDA_APP_LOGS/* ~/lambda/logs/logging/