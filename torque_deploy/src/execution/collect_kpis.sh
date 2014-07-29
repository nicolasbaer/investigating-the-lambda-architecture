#!/bin/bash

question=${1}
dataset=${2}
nodefile=${3}
parallelism=${4}
sys_start_time=${5}
data_start_time=${6}
ticks_per_ms=${7}


. ./global.sh


mongoimport_exec=$job_home/mongodb/home/bin/mongoimport
result_folder=$experiment_home/result/kpi
mkdir -p $result_folder

$mongoimport_exec --db lambda --collection baseline --file $lambda_home_dataset/$dataset/baseline/$question/baseline.json


echo "$lambda_home_pyenv/bin/python $lambda_home_exec/collect_kpis.py $result_folder $experiment_home/failure $sys_start_time $data_start_time $ticks_per_ms $parallelism"

$lambda_home_pyenv/bin/python $lambda_home_exec/collect_kpis.py "$result_folder" "$experiment_home/failure" "$sys_start_time" "$data_start_time" "$ticks_per_ms" "$parallelism"
