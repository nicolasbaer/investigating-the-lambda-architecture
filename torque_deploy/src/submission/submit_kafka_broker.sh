#!/bin/bash
#PBS -N kafka_broker
#PBS -l nodes=1:ppn=1
#PBS -j oe
#PBS -o kafka_broker_$LKAFKA_NODE_NR.out
#PBS -e kafka_broker_$LKAFKA_NODE_NR.err
#PBS -m n
#PBS -V

cd ~/lambda/execution
chmod +x ./*.sh
source ./kafka_broker.sh $LZOO_HOST $LKAFKA_NODE_NR

sleep $SECONDS_SLEEP

mkdir -p ~/lambda/logs/kafka_$PBS_JOBID
cp $LAMBDA_APP_LOGS/* ~/lambda/logs/kafka_$PBS_JOBID/