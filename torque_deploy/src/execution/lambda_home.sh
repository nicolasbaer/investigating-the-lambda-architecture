#!/bin/bash

# setup the lambda home directory


USER_HOME=/home/user/baer/lambda
LAMBDA_HOME=/home/torque/tmp/baer.$PBS_JOBID/lambda
LAMBDA_CONF=$USER_HOME/src/conf

mkdir -p $LAMBDA_HOME
