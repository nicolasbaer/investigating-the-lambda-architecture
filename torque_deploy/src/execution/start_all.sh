#!/bin/bash

. ./global.sh

cp $PBS_NODEFILE tmp/hosts

pbsdsh -uvs $lambda_home_exec/start_node.sh


