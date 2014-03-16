#!/bin/bash
#PBS -N zoo
#PBS -l nodes=1:ppn=1
#PBS -j oe
#PBS -o zoo.out
#PBS -e zoo.err
#PBS -m n
#PBS -V

cd /home/user/baer/lambda/src/execution
chmod +x ./*.sh
./zookeeper.sh

sleep 120