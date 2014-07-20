#!/bin/bash

# nsf mounted home directory of user
lambda_home=~/lambda
lambda_home_conf=$lambda_home/conf
lambda_home_exec=$lambda_home/execution
lambda_home_install=$lambda_home/install
lambda_home_pyenv=$lambda_home/pyenv
lambda_home_coordinator=$lambda_home/coordinator
lambda_home_run=$lambda_home/run
lambda_home_dataset=$lambda_home/dataset

# job scratch space
if [ -n "$SLURM_JOBID" ]
then
  job_home=/home/slurm/baer-$SLURM_JOBID/lambda
else
  job_home=/home/torque/tmp/baer.$PBS_JOBID/lambda
fi

mkdir -p $job_home


# set jobid variable
if [ -n "$SLURM_JOBID" ]
then
  jobid="$SLURM_JOBID"
else
  jobid="$PBS_JOBID"
fi

# experiment tmp folder
experiment_home=~/experiment/$jobid
mkdir -p $experiment_home


# run folder
mkdir -p $job_home/run
mkdir -p $job_home/run/speed
mkdir -p $job_home/run/coordinator
mkdir -p $job_home/run/batch

run_home=$job_home/run
batch_home=$job_home/run/batch
speed_home=$job_home/run/speed
coordinator_home=$job_home/run/coordinator