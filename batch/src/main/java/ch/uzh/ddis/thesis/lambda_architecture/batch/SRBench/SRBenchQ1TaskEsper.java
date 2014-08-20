package ch.uzh.ddis.thesis.lambda_architecture.batch.SRBench;

import ch.uzh.ddis.thesis.lambda_architecture.data.SRBench.SRBenchDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.data.TimestampedOffset;
import ch.uzh.ddis.thesis.lambda_architecture.data.esper.EsperFactory;
import ch.uzh.ddis.thesis.lambda_architecture.data.esper.EsperUpdateListener;
import ch.uzh.ddis.thesis.lambda_architecture.data.timewindow.TimeWindow;
import ch.uzh.ddis.thesis.lambda_architecture.data.timewindow.TumblingWindow;
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
import java.util.UUID;

/**
 * Samza task to solve the query `rainfall observed once an hour` on the SRBench data set.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class SRBenchQ1TaskEsper implements StreamTask, InitableTask, WindowableTask {
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");
    private static final Marker remoteDebug = MarkerManager.getMarker("DEBUGFLUME");

    private static final long shutdownWaitThreshold = (1000 * 60 * 2); // 2 minutes
    private final String uuid = UUID.randomUUID().toString();

    private static final String esperEngineName = "srbench-q1";
    private static final String esperQueryPath = "/esper-queries/srbench-q1.esper";
    private static final long windowSize = 60l * 60l * 1000l; // 1 hour

    private SystemStream resultStream;
    private static final String outputKeySerde = "string";
    private static final String outputMsgSerde = "map";

    private static final String firstTimestampStoreName = "timewindow";
    private static final String firstTimestampKey = "firstTimeStamp";
    private KeyValueStore<String, Long> firstTimestampStore;
    private boolean firstTimestampSaved = false;

    private EPServiceProvider eps;
    private EPRuntime esper;
    private TimeWindow<TimestampedOffset> timeWindow;
    private EsperUpdateListener esperUpdateListener;
    private String query;

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
        TimestampedOffset timestampedOffset = new TimestampedOffset(entry, incomingMessageEnvelope.getOffset());

        if(!firstTimestampSaved){
            logger.info(performance, "topic=firstTimeStamp ts={}", entry.getTimestamp());
            this.firstTimestampStore.put(firstTimestampKey, entry.getTimestamp());
            firstTimestampSaved = true;

            timeWindow.addEvent(timestampedOffset);
        }

        this.sendTimeEvent(entry.getTimestamp());
        this.esper.sendEvent(entry.getMap(), entry.getMeasurement());

        if(!this.timeWindow.isInWindow(timestampedOffset)) {
            this.processNewData(messageCollector);

            taskCoordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK, this.timeWindow.getWindowOffsetEvent().getOffset());
        }

        this.timeWindow.addEvent(timestampedOffset);

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
            this.timeWindow.restoreWindow(timestamp);

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

        logger.info(performance, "topic=samzawindowcall currentTime={} lastTime={}", currentTime, lastDataReceived);

        if(lastDataReceived != 0 && (currentTime - lastDataReceived) > shutdownWaitThreshold){
            this.sendTimeEvent(Long.MAX_VALUE);
            this.processNewData(messageCollector);

            logger.info(performance, "topic=samzashutdown uuid={} lastData={}", uuid, lastDataReceived);

            this.eps.destroy();

            taskCoordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
            taskCoordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
        }
    }


    private void processNewData(MessageCollector messageCollector){
        if(this.esperUpdateListener.hasNewData()){
            Pair<EventBean[], EventBean[]> eventDataTouple = this.esperUpdateListener.getNewData();
            EventBean[] newEvents = eventDataTouple.getValue0();

            for(int i = 0; i < newEvents.length; i++){
                String station = (String) newEvents[i].get("station");
                String value = String.valueOf(newEvents[i].get("value"));
                String unit = (String) newEvents[i].get("unit");

                HashMap<String, Object> result = new HashMap<>(1);
                result.put("station", station);
                result.put("value", value);
                result.put("unit", unit);
                result.put("ts_start", timeWindow.getWindowStart());
                result.put("ts_end", timeWindow.getWindowEnd());
                result.put("sys_time", System.currentTimeMillis());

                OutgoingMessageEnvelope resultMessage = new OutgoingMessageEnvelope(resultStream, outputKeySerde, outputMsgSerde, station, "1", result);
                messageCollector.send(resultMessage);
            }
        }
    }


    /**
     * Initializes the esper engine.
     */
    private void initEsper(){
        URL queryPath = EsperFactory.class.getResource(esperQueryPath);
        try {
            this.query = Resources.toString(queryPath, StandardCharsets.UTF_8);
        } catch (IOException e){
            logger.error(e);
            System.exit(1);
        }

        this.eps = EsperFactory.makeEsperServiceProviderSRBench(esperEngineName + "-" + uuid);
        EPAdministrator cepAdm = eps.getEPAdministrator();
        EPStatement cepStatement = cepAdm.createEPL(query);
        this.esperUpdateListener = new EsperUpdateListener();
        cepStatement.addListener(this.esperUpdateListener);
        this.esper = eps.getEPRuntime();
    }
}
