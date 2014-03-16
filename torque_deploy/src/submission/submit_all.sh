#!/bin/bash

### OPTIONS
YARN_DATA_NODES=2


#### HELPER FUNCTIONS
host_lookup() {
    local qstat_id=$1
    local qstat_host=$(qstat -f $qstat_id | grep exec_host | cut -d = -f 2 | tr -d ' ' | cut -d / -f 1)
    echo $qstat_host
}

wait_until_started() {
    local qstat_id=$1
    local started=false
    while [ $started = false ]; do
        local qstat_status=$(qstat | grep $qstat_id | tr -s ' ' | cut -d ' ' -f 5)
        if [ $qstat_status = "R" ]; then
           started=true
        else
            sleep 5
            echo "waiting for torque to run job..."
        fi
    done
}
#### END HELPER FUNCTIONS

# start zookeeper
echo "starting zookeeper..."
LZOO_ID=$(qsub submit_zoo.sh | grep -o '[0-9]*')
wait_until_started $LZOO_ID
LZOO_HOST=$(host_lookup $LZOO_ID)
sleep 5
echo "zookeeper started on host $LZOO_HOST"


# start yarn namenode
echo "starting yarn namenode"
LYARNN_ID=$(qsub submit_yarn_namenode.sh | grep -o '[0-9]*')
wait_until_started $LYARNN_ID
LYARNN_HOST=$(host_lookup $LYARNN_ID)
sleep 5
echo "yarn name node started on $LYARNR_HOST"
echo "yarn name node UI is available under http://$LYARNN_HOST.ifi.uzh.ch:50070"


# start yarn resourcemanager
echo "starting yarn resourcemanager"
LYARNR_ID=$(LYARN_NAMENODE_HOST=$LYARNN_HOST qsub submit_yarn_resourcemanager.sh | grep -o '[0-9]*')
wait_until_started $LYARNR_ID
LYARNR_HOST=$(host_lookup $LYARNR_ID)
sleep 5
echo "yarn resource manager started on $LYARNR_HOST"
echo "yarn resource manager UI is available under http://$LYARNR_HOST.ifi.uzh.ch:8088"

# start datanodes / nodemanagers
echo "starting $YARN_DATA_NODES yarn datanodes / nodemanagers"
for((i=1; i<=$YARN_DATA_NODES; i++)); do
    LYARN_DATA_ID=$(LYARN_NAMENODE_HOST=$LYARNN_HOST LYARNR_HOST=$LYARNR_HOST qsub submit_yarn_nodemanager.sh | grep -o '[0-9]*')
    LYARN_DATA_ARR[i]=$LYARN_DATA_ID
    echo "yarn datanode started with id $LYARN_DATA_ID"
done

for((i=1; i<=$YARN_DATA_NODES; i++)); do
    wait_until_started ${LYARN_DATA_ARR[i]}
done
sleep 5
echo "yarn datanodes / nodemanagers started"
