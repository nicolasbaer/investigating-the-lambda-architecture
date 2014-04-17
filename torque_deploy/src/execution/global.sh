#!/bin/bash

# nsf mounted home directory of user
lambda_home=~/lambda
lambda_home_conf=$lambda_home/conf
lambda_home_exec=$lambda_home/execution
lambda_home_install=$lambda_home/install
lambda_home_pyenv=$lambda_home/pyenv
lambda_home_coordinator=$lambda_home/coordinator

# job scratch space
job_home=/home/torque/tmp/baer.$PBS_JOBID/lambda

mkdir -p $job_home
