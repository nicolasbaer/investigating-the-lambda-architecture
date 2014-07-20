#!/bin/bash

dataset=$1
num_nodes=$SLURM_NNODES

. ./global.sh

mkdir -p $job_home/dataset

cp $lambda_home/dataset/$dataset/$num_nodes/*$SLURM_NODEID.csv $job_home/dataset/

copied_file=$(ls $job_home/dataset/)
echo "copied: $copied_file"
