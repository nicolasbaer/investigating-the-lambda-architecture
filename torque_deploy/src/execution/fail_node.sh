#!/bin/bash

cd ~/lambda/execution

. ./global.sh


# kill storm
kill -9 $(ps aux | grep baer | grep storm | awk '{print $2}')
rm -rf $job_home/storm/data/*

# kill kafka
kill -9 $(cat $job_home/kafka/pids/pidfile)
#rm -rf $job_home/kafka/data/*
sleep 2
ps aux | grep kafka > ~/f_log/fail_kafka-$SLURM_NODEID.log

# kill yarn
kill -9 $(ps aux | grep baer | grep yarn | awk '{print $2}')
rm -rf $job_home/yarn/data/*