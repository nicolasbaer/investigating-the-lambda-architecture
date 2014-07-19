package ch.uzh.ddis.thesis.lambda_architecture.batch.SRBench.task;

import ch.uzh.ddis.thesis.lambda_architecture.data.timewindow.TimeWindow;
import ch.uzh.ddis.thesis.lambda_architecture.data.timewindow.TumblingWindow;
import ch.uzh.ddis.thesis.lambda_architecture.data.SRBench.SRBenchDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.data.SimpleTimestamp;
import ch.uzh.ddis.thesis.lambda_architecture.data.Timestamped;
import ch.uzh.ddis.thesis.lambda_architecture.data.esper.EsperFactory;
import ch.uzh.ddis.thesis.lambda_architecture.data.esper.EsperUpdateListener;
import ch.uzh.ddis.thesis.lambda_architecture.shutdown_handler.ShutdownHandler;
import com.ecyrd.speed4j.StopWatch;
import com.espertech.esper.client.*;
import com.espertech.esper.client.time.CurrentTimeEvent;
import com.google.common.base.Optional;
import com.google.common.io.Resources;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.javatuples.Pair;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Stream Task to answer SRBench Question 1 using esper engine:
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class SRBenchQ6TaskEsper implements StreamTask, InitableTask, WindowableTask {
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");
    private static final Marker remoteDebug = MarkerManager.getMarker("DEBUGFLUME");

    private static final long shutdownWaitThreshold = (1000 * 60 * 5); // 5 minutes
    private final String uuid = UUID.randomUUID().toString();

    private static final String esperEngineName = "srbench-q6";
    private static final String esperQueryPathRainfall = "/esper-queries/srbench-q6-rainfall.esper";
    private static final String esperQueryPathSnowfall = "/esper-queries/srbench-q6-snowfall.esper";
    private static final String esperQueryPathVisibility = "/esper-queries/srbench-q6-visibility.esper";
    private static final long windowSize = 60l * 60l * 1000l; // 1 hour

    private static SystemStream resultStream;
    private static final String outputKeySerde = "string";
    private static final String outputMsgSerde = "map";

    private static final String firstTimestampStoreName = "timewindow";
    private static final String firstTimestampKey = "firstTimeStamp";
    private KeyValueStore<String, Long> firstTimestampStore;
    private boolean firstTimestampSaved = false;

    private EPRuntime esper;
    private TimeWindow<Timestamped> timeWindow;
    private EsperUpdateListener esperUpdateListenerRainfall;
    private EsperUpdateListener esperUpdateListenerSnowfall;
    private EsperUpdateListener esperUpdateListenerVisibility;
    private String queryRainfall;
    private String querySnowfall;
    private String queryVisibility;

    private long lastTimestamp = 0;
    private long lastDataReceived;
    private long processCounter = 0;
    private StopWatch processWatch;

    @Override
    public void init(Config config, TaskContext taskContext) throws Exception {
        String resultStreamName = config.get("custom.srbench.result.stream");
        this.resultStream = new SystemStream("kafka", resultStreamName);

        this.timeWindow = new TumblingWindow<>(windowSize);
        this.initEsper();

        this.firstTimestampStore = (KeyValueStore<String, Long>) taskContext.getStore(firstTimestampStoreName);
        this.restoreTimeWindow();

        this.processWatch = new StopWatch();
    }

    @Override
    public void process(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector, TaskCoordinator taskCoordinator) {
        SRBenchDataEntry entry = new SRBenchDataEntry((String) incomingMessageEnvelope.getMessage());

        if(!firstTimestampSaved){
            this.firstTimestampStore.put(firstTimestampKey, entry.getTimestamp());
            firstTimestampSaved = true;
        }

        this.sendTimeEvent(entry.getTimestamp());
        this.esper.sendEvent(entry.getMap(), entry.getMeasurement());

        if(!this.timeWindow.isInWindow(entry)) {
            this.processNewData(messageCollector);

            taskCoordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
        }

        this.timeWindow.addEvent(entry);

        this.processCounter++;
        if(this.processCounter % 1000 == 0){
            this.processWatch.stop();
            logger.info(performance, "topic={} stepSize={} duration={} timestamp={} datatimestamp={} threadId={}",
                    "samzaMessageThroughput", "1000", this.processWatch.getTimeMicros(), System.currentTimeMillis(),
                    entry.getTimestamp(), this.uuid);
            this.processWatch = new StopWatch();
        }
    }

    private void restoreTimeWindow(){
        Optional<Long> optionalTimeWindowStart = Optional.fromNullable(this.firstTimestampStore.get(firstTimestampKey));
        if(optionalTimeWindowStart.isPresent()){
            long timestamp = optionalTimeWindowStart.get();
            this.sendTimeEvent(timestamp);
            this.firstTimestampSaved = true;
            this.timeWindow.addEvent(new SimpleTimestamp(timestamp));

            logger.info(remoteDebug, "topic=samzaFirstTimestampRestore restored={} uuid={}", optionalTimeWindowStart.get());
        }
    }

    private void sendTimeEvent(long timestamp){
        if(lastTimestamp != timestamp) {
            CurrentTimeEvent timeEvent = new CurrentTimeEvent(timestamp);
            this.esper.sendEvent(timeEvent);

            this.lastTimestamp = timestamp;
            this.lastDataReceived = System.currentTimeMillis();
        }
    }


    @Override
    public void window(MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        long currentTime = System.currentTimeMillis();

        if(lastDataReceived != 0 && (currentTime - lastDataReceived) > shutdownWaitThreshold){
            this.sendTimeEvent(Long.MAX_VALUE);
            this.processNewData(messageCollector);

            logger.info(performance, "topic=samzashutdown uuid={} lastData={}", uuid, lastDataReceived);

            taskCoordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);

            ShutdownHandler.handleShutdown("layer=batch");
        }
    }


    private void processNewData(MessageCollector messageCollector){
        Set<String> stations = new HashSet<>();

        if(this.esperUpdateListenerRainfall.hasNewData()){
            Pair<EventBean[], EventBean[]> eventDataTouple = this.esperUpdateListenerRainfall.getNewData();
            EventBean[] newEvents = eventDataTouple.getValue0();

            for(int i = 0; i < newEvents.length; i++){
                String station = (String) newEvents[i].get("station");
                stations.add(station);
            }
        }

        if(this.esperUpdateListenerSnowfall.hasNewData()){
            Pair<EventBean[], EventBean[]> eventDataTouple = this.esperUpdateListenerSnowfall.getNewData();
            EventBean[] newEvents = eventDataTouple.getValue0();

            for(int i = 0; i < newEvents.length; i++){
                String station = (String) newEvents[i].get("station");
                stations.add(station);
            }
        }

        if(this.esperUpdateListenerVisibility.hasNewData()){
            Pair<EventBean[], EventBean[]> eventDataTouple = this.esperUpdateListenerVisibility.getNewData();
            EventBean[] newEvents = eventDataTouple.getValue0();

            for(int i = 0; i < newEvents.length; i++){
                String station = (String) newEvents[i].get("station");
                stations.add(station);
            }
        }

        if(!stations.isEmpty()){
            for(String station : stations){
                HashMap<String, Object> result = new HashMap<>(1);
                result.put("station", station);
                result.put("ts_start", timeWindow.getWindowStart());
                result.put("ts_end", timeWindow.getWindowEnd());
                result.put("sys_time", System.currentTimeMillis());

                OutgoingMessageEnvelope resultMessage = new OutgoingMessageEnvelope(resultStream, outputKeySerde, outputMsgSerde, "1", "1", result);
                messageCollector.send(resultMessage);
            }
        }
    }


    /**
     * Initializes the esper engine.
     */
    private void initEsper(){
        URL queryPathRainfall = EsperFactory.class.getResource(esperQueryPathRainfall);
        URL queryPathSnowfall = EsperFactory.class.getResource(esperQueryPathSnowfall);
        URL queryPathVisibility = EsperFactory.class.getResource(esperQueryPathVisibility);
        try {
            this.queryRainfall = Resources.toString(queryPathRainfall, StandardCharsets.UTF_8);
            this.querySnowfall = Resources.toString(queryPathSnowfall, StandardCharsets.UTF_8);
            this.queryVisibility = Resources.toString(queryPathVisibility, StandardCharsets.UTF_8);
        } catch (IOException e){
            logger.error(e);
            System.exit(1);
        }

        EPServiceProvider eps = EsperFactory.makeEsperServiceProviderSRBench(esperEngineName + "-" + uuid);
        EPAdministrator cepAdm = eps.getEPAdministrator();
        EPStatement cepStatementRainfall = cepAdm.createEPL(queryRainfall);
        EPStatement cepStatementVisibility = cepAdm.createEPL(queryVisibility);
        EPStatement cepStatementSnowfall = cepAdm.createEPL(querySnowfall);
        this.esperUpdateListenerRainfall = new EsperUpdateListener();
        this.esperUpdateListenerVisibility = new EsperUpdateListener();
        this.esperUpdateListenerSnowfall = new EsperUpdateListener();
        cepStatementRainfall.addListener(this.esperUpdateListenerRainfall);
        cepStatementSnowfall.addListener(this.esperUpdateListenerSnowfall);
        cepStatementVisibility.addListener(this.esperUpdateListenerVisibility);
        this.esper = eps.getEPRuntime();
    }
}
