#!/bin/bash
#PBS -N storm_supervisor
#PBS -l nodes=1:ppn=1
#PBS -j oe
#PBS -o storm_supervisor_$LSTORM_NODE_NR.out
#PBS -e storm_supervisor_$LSTORM_NODE_NR.err
#PBS -m n
#PBS -V

cd ~/lambda/execution
chmod +x ./*.sh
source ./storm_supervisor.sh $LZOO_HOST $LSTORM_NIMBUS_HOST $LSTORM_NODE_NR

sleep $SECONDS_SLEEP