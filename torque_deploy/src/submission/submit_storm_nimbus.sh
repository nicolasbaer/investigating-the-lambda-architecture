#!/bin/bash
#PBS -N storm_nimbus
#PBS -l nodes=1:ppn=1
#PBS -j oe
#PBS -o storm_nimbus.out
#PBS -e storm_nimbus.err
#PBS -m n
#PBS -V

cd ~/lambda/execution
chmod +x ./*.sh
source ./storm_nimbus.sh $LZOO_HOST

sleep $SECONDS_SLEEP