#!/bin/bash

### OPTIONS
YARN_DATA_NODES=0
KAFKA_BROKER_NODES=0
STORM_SUPERVISOR_NODES=0

# defines the number of seconds the jobs will run
SECONDS_SLEEP=180


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
            echo -e "\033[1;33mwaiting for torque to run job...\033[0m"
        fi
    done
}
#### END HELPER FUNCTIONS

# echo colors
info='\033[0;34m'
success='\033[0;32m'
no_color='\033[0m'


# start zookeeper
echo -e "${info}starting zookeeper...${no_color}"
LZOO_ID=$(SECONDS_SLEEP=$SECONDS_SLEEP qsub submit_zoo.sh | grep -o '[0-9]*')
wait_until_started $LZOO_ID
LZOO_HOST=$(host_lookup $LZOO_ID)
sleep 5
echo -e "${success}zookeeper started on host $LZOO_HOST${no_color}"


# start yarn namenode
echo -e "${info}starting yarn namenode${no_color}"
LYARNN_ID=$(SECONDS_SLEEP=$SECONDS_SLEEP qsub submit_yarn_namenode.sh | grep -o '[0-9]*')
wait_until_started $LYARNN_ID
LYARNN_HOST=$(host_lookup $LYARNN_ID)
sleep 5
echo -e "${success}yarn name node started on $LYARNR_HOST${no_color}"
echo -e "${success}yarn name node UI is available at http://$LYARNN_HOST.ifi.uzh.ch:50070${no_color}"


# start yarn resourcemanager
echo -e "${info}starting yarn resourcemanager${no_color}"
LYARNR_ID=$(SECONDS_SLEEP=$SECONDS_SLEEP LYARN_NAMENODE_HOST=$LYARNN_HOST qsub submit_yarn_resourcemanager.sh | grep -o '[0-9]*')
wait_until_started $LYARNR_ID
LYARNR_HOST=$(host_lookup $LYARNR_ID)
sleep 5
echo -e "${success}yarn resource manager started on $LYARNR_HOST${no_color}"
echo -e "${success}yarn resource manager UI is available at http://$LYARNR_HOST.ifi.uzh.ch:8088${no_color}"

# start datanodes / nodemanagers
echo -e "${info}starting $YARN_DATA_NODES yarn datanodes / nodemanagers${no_color}"
for((i=1; i<=$YARN_DATA_NODES; i++)); do
    LYARN_DATA_ID=$(SECONDS_SLEEP=$SECONDS_SLEEP LYARN_NAMENODE_HOST=$LYARNN_HOST LYARNR_HOST=$LYARNR_HOST LYARN_DATA_NR=$i qsub submit_yarn_nodemanager.sh | grep -o '[0-9]*')
    LYARN_DATA_ARR[i]=$LYARN_DATA_ID
    echo -e "${success}yarn datanode started with id $LYARN_DATA_ID${no_color}"
done

for((i=1; i<=$YARN_DATA_NODES; i++)); do
    wait_until_started ${LYARN_DATA_ARR[i]}
done
echo -e "${info}$YARN_DATA_NODES yarn datanodes/nodemanagers started${no_color}"


# start kafka brokers
echo -e "${info}starting $KAFKA_BROKER_NODES kafka broker nodes${no_color}"
for((i=1; i<=$KAFKA_BROKER_NODES; i++)); do
    LKAFKA_JOB_ID=$(SECONDS_SLEEP=$SECONDS_SLEEP LZOO_HOST=$LZOO_HOST LKAFKA_NODE_NR=$i qsub submit_kafka_broker.sh | grep -o '[0-9]*')
    LKAFKA_NODE_ARR[i]=$LKAFKA_JOB_ID
done

for((i=1; i<$KAFKA_BROKER_NODES; i++)); do
    wait_until_started ${LKAFKA_NODE_ARR[i]}
done

echo -e "${success}$KAFKA_BROKER_NODES kafka brokers started${no_color}"

# start storm nimbus
echo -e "${info}starting storm nimbus node${no_color}"
LSTORM_NIMBUS_ID=$(SECONDS_SLEEP=$SECONDS_SLEEP LZOO_HOST=$LZOO_HOST qsub submit_storm_nimbus.sh | grep -o '[0-9]*')
wait_until_started $LSTORM_NIMBUS_ID
LSTORM_NIMBUS_HOST=$(host_lookup $LSTORM_NIMBUS_ID)
sleep 5
echo -e "${success}storm nimbus node started on $LSTORM_NIMBUS_HOST${no_color}"
echo -e "${success}storm web UI is available at http://$LSTORM_NIMBUS_HOST.ifi.uzh.ch:8080${no_color}"

# start storm supervisors
echo -e "${info}starting $STORM_SUPERVISOR_NODES storm supervisor nodes${no_color}"
for((i=1; i<=$STORM_SUPERVISOR_NODES; i++)); do
    LSTORM_JOB_ID=$(SECONDS_SLEEP=$SECONDS_SLEEP LZOO_HOST=$LZOO_HOST LSTORM_NIMBUS_HOST=LSTORM_NIMBUS_HOST LSTORM_NODE_NR=$i qsub submit_storm_supervisor.sh | grep -o '[0-9]*')
    LSTORM_NODE_ARR[i]=$LSTORM_JOB_ID
done

for((i=1; i<$STORM_SUPERVISOR_NODES; i++)); do
    wait_until_started ${LSTORM_NODE_ARR[i]}
done

echo -e "${success}$STORM_SUPERVISOR_NODES storm supervisors started${no_color}"


# start logging
echo -e "${info}starting logging engine (flume, elasticsearch, kibana)${no_color}"
LLOGGING_ID=$(SECONDS_SLEEP=$SECONDS_SLEEP qsub submit_logging.sh | grep -o '[0-9]*')
wait_until_started $LLOGGING_ID
LLOGGING_HOST=$(host_lookup $LLOGGING_ID)
sleep 5
echo -e "${success}kibana web ui is available at http://$LLOGGING_HOST.ifi.uzh.ch:8081${no_color}"


# display environment variables
if [[ $_ != $0 ]]; then
    echo -e "${success}the following environment variables are available to you${no_color}"
    echo -e "${success}========================================================${no_color}"
    echo -e "${success}Variable:\t\tContent:\tDescription:${no_color}"
    echo -e "${success}LZOO_HOST\t\t$LZOO_HOST\tZookeeper host:${no_color}"
    echo -e "${success}LYARNN_HOST\t\t$LYARNN_HOST\tYarn Namenode${no_color}"
    echo -e "${success}LYARNR_HOST\t\t$LYARNR_HOST\tYarn Resource Manager${no_color}"
    echo -e "${success}LSTORM_NIMBUS_HOST\t$LSTORM_NIMBUS_HOST\tStorm Nimbus${no_color}"
    echo -e "${success}LLOGGING_HOST\t\t$LLOGGING_HOST\tFlume Host${no_color}"
fi



