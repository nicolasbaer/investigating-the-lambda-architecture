/**
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

import backtype.storm.Config;
import backtype.storm.metric.api.IMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import ch.uzh.ddis.thesis.lambda_architecture.data.IDataFactory;
import ch.uzh.ddis.thesis.lambda_architecture.data.timewindow.TimeWindow;
import kafka.message.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import ch.uzh.ddis.thesis.lambda_architecture.speed.spout.kafka.PartitionManager.KafkaMessageId;

import java.util.*;


public class KafkaSpout extends BaseRichSpout {
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");
    private static final Marker remoteDebug = MarkerManager.getMarker("DEBUGFLUME");

    private final String _uuid = UUID.randomUUID().toString();


    private SpoutOutputCollector _collector;
    private PartitionCoordinator _coordinator;
    private DynamicPartitionConnections _connections;
    private ZkState _state;

    private long _lastUpdateMs = 0;
    private int _currPartitionIndex = 0;


    private final SpoutConfig _spoutConfig;
    private final IDataFactory _factory;
    private final TimeWindow<TimestampedOffset> _timeWindow;


    public KafkaSpout(SpoutConfig spoutConf, IDataFactory factory, TimeWindow<TimestampedOffset> timeWindow) {
        _spoutConfig = spoutConf;
        _factory = factory;
        _timeWindow = timeWindow;
    }

    @Override
    public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        _collector = collector;

        Map stateConf = new HashMap(conf);
        List<String> zkServers = _spoutConfig.zkServers;
        if (zkServers == null) {
            zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        }
        Integer zkPort = _spoutConfig.zkPort;
        if (zkPort == null) {
            zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
        }
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, _spoutConfig.zkRoot);
        _state = new ZkState(stateConf);

        _connections = new DynamicPartitionConnections(_spoutConfig, KafkaUtils.makeBrokerReader(conf, _spoutConfig));

        // using TransactionalState like this is a hack
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        if (_spoutConfig.hosts instanceof StaticHosts) {
            _coordinator = new StaticCoordinator(_connections, conf, _spoutConfig, _state, context.getThisTaskIndex(), totalTasks, _uuid);
        } else {
            _coordinator = new ZkCoordinator(_connections, conf, _spoutConfig, _state, context.getThisTaskIndex(), totalTasks, _uuid, _timeWindow, _factory);
        }

        context.registerMetric("kafkaOffset", new IMetric() {
            KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(_spoutConfig.topic, _connections);

            @Override
            public Object getValueAndReset() {
                List<PartitionManager> pms = _coordinator.getMyManagedPartitions();
                Set<Partition> latestPartitions = new HashSet();
                for (PartitionManager pm : pms) {
                    latestPartitions.add(pm.getPartition());
                }
                _kafkaOffsetMetric.refreshPartitions(latestPartitions);
                for (PartitionManager pm : pms) {
                    _kafkaOffsetMetric.setLatestEmittedOffset(pm.getPartition(), pm.lastCompletedOffset());
                }
                return _kafkaOffsetMetric.getValueAndReset();
            }
        }, _spoutConfig.metricsTimeBucketSizeInSecs);

        context.registerMetric("kafkaPartition", new IMetric() {
            @Override
            public Object getValueAndReset() {
                List<PartitionManager> pms = _coordinator.getMyManagedPartitions();
                Map concatMetricsDataMaps = new HashMap();
                for (PartitionManager pm : pms) {
                    concatMetricsDataMaps.putAll(pm.getMetricsDataMap());
                }
                return concatMetricsDataMaps;
            }
        }, _spoutConfig.metricsTimeBucketSizeInSecs);
    }

    @Override
    public void close() {
        _state.close();
    }

    @Override
    public void nextTuple() {
        List<PartitionManager> managers = _coordinator.getMyManagedPartitions();
        for (int i = 0; i < managers.size(); i++) {

            try {
                // in case the number of managers decreased
                _currPartitionIndex = _currPartitionIndex % managers.size();
                EmitState state = managers.get(_currPartitionIndex).next(_collector);
                if (state != EmitState.EMITTED_MORE_LEFT) {
                    _currPartitionIndex = (_currPartitionIndex + 1) % managers.size();
                }
                if (state != EmitState.NO_EMITTED) {
                    break;
                }
            } catch (FailedFetchException e) {
                logger.warn("Fetch failed", e);
                _coordinator.refresh();
            }
        }
    }

    @Override
    public void ack(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        PartitionManager m = _coordinator.getManager(id.partition);
        if (m != null) {
            m.ack(id.offset);
        }
    }

    @Override
    public void fail(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        PartitionManager m = _coordinator.getManager(id.partition);
        if (m != null) {
            m.fail(id.offset);
        }
    }

    @Override
    public void deactivate() {
        // no-op
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_spoutConfig.scheme.getOutputFields());
    }

    public static class MessageAndRealOffset {
        public Message msg;
        public long offset;

        public MessageAndRealOffset(Message msg, long offset) {
            this.msg = msg;
            this.offset = offset;
        }
    }

    static enum EmitState {
        EMITTED_MORE_LEFT,
        EMITTED_END,
        NO_EMITTED
    }

}