#!/bin/bash
#PBS -N kafka_broker
#PBS -l nodes=1:ppn=1
#PBS -j oe
#PBS -o kafka_broker.out
#PBS -e kafka_broker.err
#PBS -m n
#PBS -V

cd /home/user/baer/lambda/src/execution
chmod +x ./*.sh
LZOO_HOST=$LZOO_HOST source ./storm_nimbus.sh

sleep 120