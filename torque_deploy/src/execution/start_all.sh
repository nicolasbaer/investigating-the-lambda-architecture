#!/bin/bash

if [ -z "$6" ]
then
  echo "please specify all parameters: dataset, dataset_name, parallelism, experiment, question, ticksPerMs"
  exit 1
fi

dataset=$1
dataset_name=$2
parallelism=$3
experiment=$4
question=$5
ticksPerMs=$6
kill_probability=$7
kill_interval=$8
realive_interval=$9
kill_concurrent_nodes=${10}

. ./global.sh

chmod +x $lambda_home_exec/*.sh

shutdown_folder=~/shutdown_handler
mkdir -p $shutdown_folder
rm -rf $shutdown_folder/*

nodefile=$experiment_home/nodefile

if [ -n "$SLURM_JOBID" ]
then
  scontrol show hostname "$SLURM_NODELIST" > $nodefile
  
  # start all services
  srun $lambda_home_exec/start_node.sh "$nodefile"
  
  # copy dataset
  srun $lambda_home_exec/copy_dataset.sh "$dataset"
  
  # start processing layer
  if [ $experiment = "speed" ]
  then
    $lambda_home_exec/speed_layer.sh $dataset_name $question
  fi

  if [ $experiment = "batch-samza" ]
  then
    $lambda_home_exec/batch_samza_layer.sh $question
  fi

  if [ $experiment = "batch-storm" ]
  then
    $lambda_home_exec/batch_storm_layer.sh $dataset_name $question
  fi

  # start time of the coordinator is delayed by 10 seconds and converted to java format (ms)
  start_time=$(date +%s)
  start_time=$((start_time+10))
  start_time=$((start_time*1000))
  dataset_start_time=$(<$lambda_home_dataset/$dataset/start_time)

  mkdir -p $experiment_home/runtime
  echo "$start_time" > $experiment_home/runtime/start_time_sys
  echo "$dataset_start_time" > $experiment_home/runtime/start_time_data
  echo "$ticksPerMs" > $experiment_home/runtime/ticks
  echo "$experiment" > $experiment_home/runtime/experiment
  echo "$dataset" > $experiment_home/runtime/dataset
  echo "$question" > $experiment_home/runtime/question
  echo "$kill_probability" > $experiment_home/runtime/kill_probability
  echo "$kill_interval" > $experiment_home/runtime/kill_interval
  echo "$realive_interval" > $experiment_home/runtime/realive_interval


  # start producer and node failure simulation on each node:
  failure_log_folder=$experiment_home/failure
  mkdir -p $failure_log_folder
  srun $lambda_home_exec/start_producer.sh $experiment $dataset $dataset_name $ticksPerMs $start_time $dataset_start_time $kill_probability $kill_interval $parallelism $shutdown_folder $failure_log_folder $realive_interval $nodefile $kill_concurrent_nodes

  # store results
  $lambda_home_exec/store_result.sh $question

  # store logging output
  $lambda_home_exec/store_logging.sh

  # stop all services
  srun $lambda_home_exec/stop_node.sh

else
  cp $PBS_NODEFILE > $nodefile
  pbsdsh -uvs $lambda_home_exec/start_node.sh "$nodefile"

  sleep 120
  srun $lambda_home_exec/stop_node.sh
fi
