#!/bin/bash

question=$1

. ./global.sh


mongoexport_exec=$job_home/mongodb/home/bin/mongoexport
result_folder=$experiment_home/result
mkdir -p result_folder

fields=""
if [ "$question" = "srbench-q1" ]
then
  fields="station,value,unit,ts_start,ts_end"
fi

if [ "$question" = "srbench-q2" ]
then
  fields="station,value,unit,ts_start,ts_end"
fi

if [ "$question" = "srbench-q3" ]
then
  fields="station,ts_start,ts_end"
fi

if [ "$question" = "srbench-q4" ]
then
  fields="station,speed,temperature,ts_start,ts_end"
fi

if [ "$question" = "srbench-q5" ]
then
  fields="station,wind,airTemperature,ts_start,ts_end"
fi

if [ "$question" = "srbench-q6" ]
then
  fields="station,ts_start,ts_end"
fi

if [ "$question" = "srbench-q7" ]
then
  fields="station,ts_start,ts_end"
fi

if echo $question | grep -q "debs-q1-house";
then
  fields="ts,house_id,predicted_load,sys_time,ts_start,ts_end"
fi

if echo $question | grep -q "debs-q1-plug";
then
  fields="ts,house_id,household_id,plug_id,predicted_load,sys_time,ts_start,ts_end"
fi

if echo $question | grep -q "debs-q3-house";
then
  fields="house_id,load,sys_time,ts_start,ts_end"
fi

$mongoexport_exec --db lambda --collection result --out $result_folder/result.json
$mongoexport_exec --db lambda --collection result --csv --out $result_folder/result.csv --fields $fields

