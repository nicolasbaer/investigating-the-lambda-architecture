#!/bin/bash
#PBS -N yarn_rm
#PBS -l nodes=1:ppn=1
#PBS -j oe
#PBS -o yarn_rm.out
#PBS -e yarn_rm.err
#PBS -m n
#PBS -V

cd /home/user/baer/lambda/src/execution
chmod +x ./*.sh
LYARN_NAMENODE_HOST=$LYARN_NAMENODE_HOST source ./yarn_resourcemanager.sh

sleep 120

cp $LYARN_HOME/etc/hadoop/core-site.xml /home/user/baer/lambda/logs/
cp $LYARN_LOGS/rm.log /home/user/baer/lambda/logs/
