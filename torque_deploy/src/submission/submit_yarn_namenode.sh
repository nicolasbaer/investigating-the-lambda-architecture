#!/bin/bash
#PBS -N yarn_nn
#PBS -l nodes=1:ppn=1
#PBS -j oe
#PBS -o yarn_nn.out
#PBS -e yarn_nn.err
#PBS -m n
#PBS -V

cd /home/user/baer/lambda/src/execution
chmod +x ./*.sh
. ./yarn_namenode.sh

sleep 120

cp $LYARN_LOGS/namenode.log /home/user/baer/lambda/logs/