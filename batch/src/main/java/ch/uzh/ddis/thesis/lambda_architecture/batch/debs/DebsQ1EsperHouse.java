package ch.uzh.ddis.thesis.lambda_architecture.batch.DEBS;

import ch.uzh.ddis.thesis.lambda_architecture.data.TimestampedOffset;
import ch.uzh.ddis.thesis.lambda_architecture.data.debs.DebsDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.data.esper.EsperFactory;
import ch.uzh.ddis.thesis.lambda_architecture.data.esper.EsperUpdateListener;
import ch.uzh.ddis.thesis.lambda_architecture.data.timewindow.TimeWindow;
import ch.uzh.ddis.thesis.lambda_architecture.data.timewindow.TumblingWindow;
import ch.uzh.ddis.thesis.lambda_architecture.data.utils.Round;
import com.ecyrd.speed4j.StopWatch;
import com.espertech.esper.client.*;
import com.espertech.esper.client.time.CurrentTimeEvent;
import com.google.common.base.Optional;
import com.google.common.io.Resources;
import org.apache.commons.math3.stat.StatUtils;
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
 * Samza task to solve the query `load prediction` on the debs data set.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class DebsQ1EsperHouse implements StreamTask, InitableTask, WindowableTask {
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");
    private static final Marker remoteDebug = MarkerManager.getMarker("DEBUGFLUME");

    private static final long shutdownWaitThreshold = (1000 * 60 * 5); // 2 minutes
    private final String uuid = UUID.randomUUID().toString();

    private static final String esperEngineName = "debs-q1-house";
    private static final String esperQueryPath = "/esper-queries/debs-q1-house.esper";
    private static final String debsWindowSizeConf = "custom.debs.window.size";
    private long windowSize = 0;
    private long windowSizeMinutes = 0;

    private SystemStream resultStream;
    private static final String outputKeySerde = "string";
    private static final String outputMsgSerde = "map";

    private static final String firstTimestampStoreName = "timewindow";
    private static final String firstTimestampKey = "firstTimeStamp";
    private KeyValueStore<String, Long> firstTimestampStore;
    private boolean firstTimestampSaved = false;

    private static final String historicalDataStoreName = "history";
    private KeyValueStore<String, Double> historyStore;

    private EPRuntime esper;
    private EPServiceProvider eps;
    private TimeWindow<TimestampedOffset> timeWindow;
    private EsperUpdateListener esperUpdateListener;
    private String query;

    private long lastTimestamp = 0;
    private long lastDataReceived = 0;
    private long processCounter = 0;
    private StopWatch processWatch;

    @Override
    public void init(Config config, TaskContext taskContext) throws Exception {
        this.windowSizeMinutes = Long.valueOf(config.get(debsWindowSizeConf));
        this.windowSize = windowSizeMinutes * 60 * 1000;

        this.resultStream = new SystemStream("kafka", "result");

        this.timeWindow = new TumblingWindow<>(windowSize);
        this.initEsper();

        this.firstTimestampStore = (KeyValueStore<String, Long>) taskContext.getStore(firstTimestampStoreName);
        this.restoreTimeWindow();

        this.historyStore = (KeyValueStore<String, Double>) taskContext.getStore(historicalDataStoreName);

        this.processWatch = new StopWatch();
    }

    @Override
    public void process(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector, TaskCoordinator taskCoordinator) {
        DebsDataEntry entry = new DebsDataEntry((String) incomingMessageEnvelope.getMessage());
        TimestampedOffset timestampedOffset = new TimestampedOffset(entry, incomingMessageEnvelope.getOffset());

        if(!firstTimestampSaved){
            this.firstTimestampStore.put(firstTimestampKey, entry.getTimestamp());
            firstTimestampSaved = true;

            logger.info(performance, "topic=firstTimestamp ts={} partition={} offset={} container={}", entry.getTimestamp(), incomingMessageEnvelope.getSystemStreamPartition().getPartition().getPartitionId(), incomingMessageEnvelope.getOffset(), System.getenv("SAMZA_CONTAINER_NAME"));

            timeWindow.addEvent(timestampedOffset);
        }

        this.sendTimeEvent(entry.getTimestamp());
        this.esper.sendEvent(entry.getMap(), entry.getType().toString());

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

            logger.info(performance, "topic=samzashutdown uuid={} lastData={} current={}", uuid, lastDataReceived, currentTime);

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
                String houseId = String.valueOf(newEvents[i].get("houseId"));
                Double load = (Double) newEvents[i].get("load");

                if(load == null){
                    continue;
                }

                load = Round.roundToFiveDecimals(load);

                // save into kv-store
                String key = new StringBuilder().append(this.timeWindow.getWindowStart()).append("-").append(houseId).toString();
                historyStore.put(key, load);

                // retrieve historical values
                long times[] = new long[3];
                long nextPrediction = this.timeWindow.getWindowStart() + (this.windowSize * 2);
                long oneDay = 24l * 60l * 60l * 1000l;
                times[0] = (nextPrediction) - (oneDay * 3);
                times[1] = (nextPrediction) - (oneDay * 2);
                times[2] = (nextPrediction) - (oneDay * 1);

                double values[] = new double[3];
                for(int j = 0; j < times.length; j++){
                    long t = times[j];
                    try {
                        String keyT = new StringBuilder().append(t).append("-").append(houseId).toString();
                        Optional<Double> optionalValue = Optional.of(this.historyStore.get(keyT));
                        values[j] = optionalValue.get();
                    }catch (Exception e){
                        // in case there's no value stored for the given time we use the current value
                        values[j] = load;
                    }
                }

                // calculate the result according to L(s_{i+2}) = ( avgLoad(s_i) + median( { avgLoad(s_j) } ) ) / 2
                double predictedLoad = (load + StatUtils.percentile(values, 50)) / 2;
                predictedLoad = Round.roundToFiveDecimals(predictedLoad);

                HashMap<String, Object> result = new HashMap<>(1);
                result.put("ts", nextPrediction / 1000);
                result.put("house_id", houseId);
                result.put("predicted_load", predictedLoad);
                result.put("sys_time", System.currentTimeMillis());
                result.put("ts_start", this.timeWindow.getWindowStart());
                result.put("ts_end", this.timeWindow.getWindowEnd());

                OutgoingMessageEnvelope resultMessage = new OutgoingMessageEnvelope(resultStream, outputKeySerde, outputMsgSerde, houseId, "1", result);
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
            this.query = query.replace("%MINUTES%", String.valueOf(this.windowSizeMinutes));
        } catch (IOException e){
            logger.error(e);
            System.exit(1);
        }

        this.eps = EsperFactory.makeEsperServiceProviderDebs(esperEngineName + "-" + uuid);
        EPAdministrator cepAdm = eps.getEPAdministrator();
        EPStatement cepStatement = cepAdm.createEPL(query);
        this.esperUpdateListener = new EsperUpdateListener();
        cepStatement.addListener(this.esperUpdateListener);
        this.esper = eps.getEPRuntime();
    }
}
