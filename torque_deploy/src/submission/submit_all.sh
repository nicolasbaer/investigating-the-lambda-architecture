#!/bin/bash

### OPTIONS
YARN_DATA_NODES=5
KAFKA_BROKER_NODES=1


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
echo "yarn name node UI is available at http://$LYARNN_HOST.ifi.uzh.ch:50070"


# start yarn resourcemanager
echo "starting yarn resourcemanager"
LYARNR_ID=$(LYARN_NAMENODE_HOST=$LYARNN_HOST qsub submit_yarn_resourcemanager.sh | grep -o '[0-9]*')
wait_until_started $LYARNR_ID
LYARNR_HOST=$(host_lookup $LYARNR_ID)
sleep 5
echo "yarn resource manager started on $LYARNR_HOST"
echo "yarn resource manager UI is available at http://$LYARNR_HOST.ifi.uzh.ch:8088"

# start datanodes / nodemanagers
echo "starting $YARN_DATA_NODES yarn datanodes / nodemanagers"
for((i=1; i<=$YARN_DATA_NODES; i++)); do
    LYARN_DATA_ID=$(LYARN_NAMENODE_HOST=$LYARNN_HOST LYARNR_HOST=$LYARNR_HOST LYARN_DATA_NR=$i qsub submit_yarn_nodemanager.sh | grep -o '[0-9]*')
    LYARN_DATA_ARR[i]=$LYARN_DATA_ID
    echo "yarn datanode started with id $LYARN_DATA_ID"
done

for((i=1; i<=$YARN_DATA_NODES; i++)); do
    wait_until_started ${LYARN_DATA_ARR[i]}
done
echo "yarn datanodes / nodemanagers started"


# start kafka brokers
echo "starting $KAFKA_BROKER_NODES kafka broker nodes"
for((i=1; i<$KAFKA_BROKER_NODES; i++)); do
    LKAFKA_JOB_ID=$(LZOO_HOST=$LZOO_HOST LKAFKA_NODE_NR=$i qsub submit_kafka_broker.sh | grep -o '[0-9]*')
    LKAFKA_NODE_ARR[i]=$LKAFKA_JOB_ID
done

for((i=1; i<$KAFKA_BROKER_NODES; i++)); do
    wait_until_started ${LKAFKA_NODE_ARR[i]}
done

echo "kafka brokers started"

# start storm nimbus
echo "starting storm nimbus node"
LSTORM_NIMBUS_ID=$(LZOO_HOST=$LZOO_HOST qsub submit_storm_nimbus.sh | grep -o '[0-9]*')
wait_until_started $LSTORM_NIMBUS_ID
LSTORM_NIMBUS_HOST=$(host_lookup $LSTORM_NIMBUS_ID)
sleep 5
echo "storm nimbus node started on $LSTORM_NIMBUS_HOST"
echo "storm web UI is available at http://$LSTORM_NIMBUS_HOST:8080"