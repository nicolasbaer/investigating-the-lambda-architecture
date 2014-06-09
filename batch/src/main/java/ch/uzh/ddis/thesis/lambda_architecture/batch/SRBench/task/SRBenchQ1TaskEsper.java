package ch.uzh.ddis.thesis.lambda_architecture.batch.SRBench.task;

import ch.uzh.ddis.thesis.lambda_architecture.batch.time_window.TimeWindow;
import ch.uzh.ddis.thesis.lambda_architecture.batch.time_window.TumblingWindow;
import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.data.SRBench.SRBenchDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.data.esper.EsperFactory;
import ch.uzh.ddis.thesis.lambda_architecture.data.esper.EsperUpdateListener;
import com.ecyrd.speed4j.StopWatch;
import com.espertech.esper.client.*;
import com.espertech.esper.client.time.CurrentTimeEvent;
import com.google.common.io.Resources;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.javatuples.Pair;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * Stream Task to answer SRBench Question 1 using esper engine:
 * `Get the rainfall observed once in an hour`
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class SRBenchQ1TaskEsper implements StreamTask, InitableTask, WindowableTask {
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");
    private static final Marker remoteDebug = MarkerManager.getMarker("DEBUGFLUME");

    private static final String esperEngineName = "srbench-q1";
    private static final String esperQueryPath = "/esper-queries/srbench-q1.esper";
    private static final long shutdownWaitThreshold = (1000 * 60 * 5); // 5 minutes

    private static final SystemStream resultStream = new SystemStream("kafka", "srbench-q1-results");
    private static final String outputKeySerde = "org.apache.samza.serializers.StringSerdeFactory";
    private static final String outputMsgSerde = "ch.uzh.ddis.thesis.lambda_architecture.data.serde.MapSerdeFactory";

    private EPRuntime esper;
    private TimeWindow<IDataEntry> timeWindow;
    private EsperUpdateListener esperUpdateListener;
    private String query;

    private long lastTimestamp = 0;
    private long lastDataReceived;
    private long processCounter = 0;
    private StopWatch processWatch;

    @Override
    public void init(Config config, TaskContext taskContext) throws Exception {
        long windowSize = 60l * 60l * 1000l;
        this.timeWindow = new TumblingWindow<>(windowSize);
        this.initEsper();

        this.processWatch = new StopWatch();
    }

    @Override
    public void process(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector, TaskCoordinator taskCoordinator) {

        SRBenchDataEntry entry = (SRBenchDataEntry) incomingMessageEnvelope.getMessage();

        this.sendTimeEvent(entry.getTimestamp());
        this.esper.sendEvent(entry);

        if(!this.timeWindow.isInWindow(entry)) {
            logger.info(remoteDebug, "topic=samzaProcessData newdata={} windowEnd={} currentEvent={}",
                    this.esperUpdateListener.hasNewData(), this.timeWindow.getWindowEnd(), entry.getTimestamp());
            this.processNewData(messageCollector);

            taskCoordinator.commit();
        }

        this.timeWindow.addEvent(entry);
        logger.info(remoteDebug, "topic=samzaTimeWindow start={} end={} currentData={}", timeWindow.getWindowStart(), timeWindow.getWindowEnd(), entry.getTimestamp());

        if(this.processCounter == 0 || this.processCounter % 1000 == 0){
            this.processWatch.stop();
            logger.info(performance, "topic={} stepSize={} duration={} timestamp={} datatimestamp={}",
                    "samzaProcessStep", "1000", this.processWatch.getTimeMicros(), System.currentTimeMillis(),
                    entry.getTimestamp());
            this.processWatch = new StopWatch();
        }

        logger.info(remoteDebug, "topic=samzaTimeOrder partition={} counter={} timeData={} windowStart={} windowEnd={}", incomingMessageEnvelope.getSystemStreamPartition().getPartition().getPartitionId(), processCounter, entry.getTimestamp(),timeWindow.getWindowStart(), timeWindow.getWindowEnd());

        this.processCounter++;
    }

    private void sendTimeEvent(long timestamp){
        if(lastTimestamp != timestamp) {
            CurrentTimeEvent timeEvent = new CurrentTimeEvent(timestamp);
            this.esper.sendEvent(timeEvent);

            this.lastTimestamp = timestamp;
            this.lastDataReceived = System.currentTimeMillis();

            logger.info(remoteDebug, "topic=samzaSendTimeEvent currTime={} lastTime={}", timestamp, lastDataReceived);
        }
    }


    @Override
    public void window(MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        long currentTime = System.currentTimeMillis();

        logger.info(remoteDebug, "topic=samzaWindowCall called={} lastData={}", currentTime, this.lastDataReceived);

        if(lastDataReceived != 0 && (currentTime - lastDataReceived) > shutdownWaitThreshold){
            this.sendTimeEvent(Long.MAX_VALUE);
            this.processNewData(messageCollector);


            logger.info(performance, "topic={} sysTime={} lastData={}", "samzaProcessShutdown", currentTime, this.lastDataReceived);

            taskCoordinator.shutdown(TaskCoordinator.ShutdownMethod.WAIT_FOR_ALL_TASKS);
        }
    }


    private void processNewData(MessageCollector messageCollector){
        if(this.esperUpdateListener.hasNewData()){
            Pair<EventBean[], EventBean[]> eventDataTouple = this.esperUpdateListener.getNewData();
            EventBean[] newEvents = eventDataTouple.getValue0();

            for(int i = 0; i < newEvents.length; i++){
                String station = (String) newEvents[i].get("station");
                String value = (String) newEvents[i].get("value");
                String unit = (String) newEvents[i].get("unit");

                HashMap<String, Object> result = new HashMap<>(1);
                result.put("station", station);
                result.put("value", value);
                result.put("unit", unit);

                OutgoingMessageEnvelope resultMessage = new OutgoingMessageEnvelope(resultStream, outputKeySerde, outputMsgSerde, result);
                messageCollector.send(resultMessage);

                logger.info(remoteDebug, "topic=samzaSendResult");
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

        EPServiceProvider eps = EsperFactory.makeEsperServiceProviderSRBench(esperEngineName);
        EPAdministrator cepAdm = eps.getEPAdministrator();
        EPStatement cepStatement = cepAdm.createEPL(query);
        this.esperUpdateListener = new EsperUpdateListener();
        cepStatement.addListener(this.esperUpdateListener);
        this.esper = eps.getEPRuntime();
    }
}
