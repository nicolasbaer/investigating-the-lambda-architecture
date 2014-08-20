#!/bin/bash

nodefile=$1

. ./global.sh

$lambda_home_pyenv/bin/python starter.py configure_node $lambda_home_exec/cluster_layout.json $lambda_node_num $lambda_node_num $nodefile $lambda_home_exec


