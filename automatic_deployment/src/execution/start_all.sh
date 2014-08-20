#!/bin/bash

# this is the start script of the automatic deployment and runs an experiment. please see below for the mandatory set
# of parameters.


if [ -z "${10}" ]
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


chmod +x $lambda_home_exec/*.sh

. ./global.sh


shutdown_folder=~/shutdown_handler
mkdir -p $shutdown_folder
rm -rf $shutdown_folder/*


nodefile=$experiment_home/nodefile

# cluster management specifics
if [ -n "$SLURM_JOBID" ]
then
  parallel_run="srun"
  scontrol show hostname "$SLURM_NODELIST" > $nodefile
else
  parallel_run="pbsdsb"
  cp $PBS_NODEFILE > $nodefile
fi


# start all services
eval $parallel_run $lambda_home_exec/start_node.sh "$nodefile"

# copy dataset
eval $parallel_run $lambda_home_exec/copy_dataset.sh "$dataset"

# start processing layer
if [ $experiment = "speed" ]
then
  $lambda_home_exec/run_speed_layer.sh $dataset_name $question
fi

if [ $experiment = "batch-samza" ]
then
  $lambda_home_exec/run_batch_samza_layer.sh $question
fi


sleep 15

# start time of the coordinator is delayed by 10 seconds and converted to java format (ms)
start_time=$(date +%s)
start_time=$((start_time+10))
start_time=$((start_time*1000))
dataset_start_time=$(<$lambda_home_dataset/$dataset/start_time)

# store all parameters that are supplied to this experiment
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
echo "$kill_concurrent_nodes" > $experiment_home/runtime/concurrent_nodes_kill


# start producer and node failure simulation on each node:
failure_log_folder=$experiment_home/failure
mkdir -p $failure_log_folder
eval $parallel_run $lambda_home_exec/run_producer.sh $experiment $dataset $dataset_name $ticksPerMs $start_time $dataset_start_time $kill_probability $kill_interval $parallelism $shutdown_folder $failure_log_folder $realive_interval $nodefile $kill_concurrent_nodes

# calculate kpis
$lambda_home_exec/collect_kpis.sh $question $dataset $nodefile $parallelism $start_time $dataset_start_time $ticksPerMs

# store results
$lambda_home_exec/store_result.sh $question

# store logging output
$lambda_home_exec/store_logging.sh

# stop all services
eval $parallel_run $lambda_home_exec/stop_node.sh