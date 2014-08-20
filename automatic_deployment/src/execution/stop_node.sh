#!/bin/zsh


echo "running processes:"
echo "=================="
ps aux | grep baer
echo "=================="


for x in 1 2 3
do
  processes=($(ps aux | grep baer | awk '{print $2}'))
  for p in $processes
  do
    if [ "$p" != "$SLURM_TASK_PID"  ]; then
      kill -9 $p
    fi
  done
  sleep 5
done
