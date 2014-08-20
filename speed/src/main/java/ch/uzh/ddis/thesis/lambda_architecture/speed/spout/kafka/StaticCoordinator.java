/**
 *
 * Please not that the Kafka package from Storm was copied here in order to modify it for the use case. It
 * was not possible to deeply modify its behavior regarding reliable offset commits with its restrictive access
 * policy. The code was copied from: https://github.com/apache/incubator-storm/tree/master/external/storm-kafka
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ch.uzh.ddis.thesis.lambda_architecture.speed.spout.kafka;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataFactory;
import ch.uzh.ddis.thesis.lambda_architecture.data.timewindow.TimeWindow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class StaticCoordinator implements PartitionCoordinator {
    Map<Partition, PartitionManager> _managers = new HashMap<Partition, PartitionManager>();
    List<PartitionManager> _allManagers = new ArrayList();

    public StaticCoordinator(DynamicPartitionConnections connections, Map stormConf, SpoutConfig config, ZkState state, int taskIndex, int totalTasks, String topologyInstanceId, TimeWindow<TimestampedOffset> timeWindow, IDataFactory factory) {
        StaticHosts hosts = (StaticHosts) config.hosts;
        List<Partition> myPartitions = KafkaUtils.calculatePartitionsForTask(hosts.getPartitionInformation(), totalTasks, taskIndex);
        for (Partition myPartition : myPartitions) {
            _managers.put(myPartition, new PartitionManager(connections, topologyInstanceId, state, stormConf, config, myPartition, timeWindow, factory));
        }
        _allManagers = new ArrayList(_managers.values());
    }

    @Override
    public List<PartitionManager> getMyManagedPartitions() {
        return _allManagers;
    }

    public PartitionManager getManager(Partition partition) {
        return _managers.get(partition);
    }

    @Override
    public void refresh() { return; }

}
