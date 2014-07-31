package ch.uzh.ddis.thesis.lambda_architecture.batch;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataFactory;
import ch.uzh.ddis.thesis.lambda_architecture.data.Timestamped;
import ch.uzh.ddis.thesis.lambda_architecture.data.esper.EsperFactory;
import ch.uzh.ddis.thesis.lambda_architecture.data.esper.EsperUpdateListener;
import ch.uzh.ddis.thesis.lambda_architecture.data.timewindow.TimeWindow;
import com.ecyrd.speed4j.StopWatch;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.UUID;

/**
 * Calculate the average load for each house in a tumbling window. The tumpling window size
 * is configurable with the samza properties file `custom.debs.window.size`.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class GenericEsperSamzaTask {
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");
    private static final Marker remoteDebug = MarkerManager.getMarker("DEBUGFLUME");

    private final String uuid = UUID.randomUUID().toString();
    private final String taskName;
    private final IDataFactory dataFactory;

    // esper
    private final EPRuntime esper;
    private final EPServiceProvider esperServiceProvider;
    private final TimeWindow<Timestamped> timeWindow;
    private final EsperUpdateListener esperUpdateListener;

    // first timestamp storage provider
    private static final String firstTimestampKey = "firstTimeStamp";
    private KeyValueStore<String, Long> firstTimestampStore;
    private boolean firstTimestampSaved = false;

    // performance measurement
    private long processCounter = 0;
    private StopWatch processWatch;

    // shutdown management
    private static final long shutdownWaitThreshold = (1000 * 60 * 2); // 2 minutes
    private long lastTimestamp = 0;
    private long lastDataReceived;
    private boolean stopped = false;


    public GenericEsperSamzaTask(IDataFactory dataFactory, String taskName, KeyValueStore<String, Long> firstTimestampStore, TimeWindow<Timestamped> timeWindow, String query){
        this.taskName = taskName;
        this.dataFactory = dataFactory;
        this.timeWindow = timeWindow;
        this.firstTimestampStore = firstTimestampStore;

        // init esper
        this.esperServiceProvider = EsperFactory.makeEsperServiceProviderDebs(taskName + "-" + uuid);
        EPAdministrator cepAdm = esperServiceProvider.getEPAdministrator();
        EPStatement cepStatement = cepAdm.createEPL(query);
        this.esperUpdateListener = new EsperUpdateListener();
        cepStatement.addListener(this.esperUpdateListener);
        this.esper = esperServiceProvider.getEPRuntime();

        // restore last timewindow from storage
        //this.restoreTimeWindow();
    }
/*
    @Override
    public void process(String message) {
        IDataEntry entry = dataFactory.makeDataEntryFromCSV(message);

        if(!firstTimestampSaved){
            this.firstTimestampStore.put(firstTimestampKey, entry.getTimestamp());
            firstTimestampSaved = true;

            timeWindow.addEvent(entry);
        }

        this.sendTimeEvent(entry.getTimestamp());
        this.esper.sendEvent(entry.getMap(), entry.getTypeStr());

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

        logger.info(performance, "topic=samzawindowcall currentTime={} lastTime={}", currentTime, lastDataReceived);

        if(lastDataReceived != 0 && (currentTime - lastDataReceived) > shutdownWaitThreshold){
            this.sendTimeEvent(Long.MAX_VALUE);
            this.processNewData(messageCollector);

            logger.info(performance, "topic=samzashutdown uuid={} lastData={}", uuid, lastDataReceived);

            this.esperServiceProvider.destroy();

            taskCoordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
            taskCoordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);

            if(!stopped) {
                ShutdownHandler.handleShutdown("layer=batch");
                stopped = true;
            }
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

                HashMap<String, Object> result = new HashMap<>(1);
                result.put("house_id", houseId);
                result.put("load", load);
                result.put("sys_time", System.currentTimeMillis());
                result.put("ts_start", this.timeWindow.getWindowStart());
                result.put("ts_end", this.timeWindow.getWindowEnd());

                OutgoingMessageEnvelope resultMessage = new OutgoingMessageEnvelope(resultStream, outputKeySerde, outputMsgSerde, houseId, "1", result);
                messageCollector.send(resultMessage);
            }
        }
    }
    */
}
