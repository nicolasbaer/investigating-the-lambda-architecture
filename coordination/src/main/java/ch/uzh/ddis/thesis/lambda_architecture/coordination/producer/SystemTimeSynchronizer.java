package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;
import com.ecyrd.speed4j.StopWatch;
import com.lmax.disruptor.EventHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

/**
 * Synchronizes the production rate of data entries with the system time.
 * A sensor can send entries to this object in order to pipe it to a producer with respect to a certain sending rate, which
 * is bound to the system time.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class SystemTimeSynchronizer implements EventHandler<IDataEntry> {
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");
    private static final String lagTopic = "synchronizerlag";

    private final IProducer producer;
    private final long systemTimeStart;
    private final long ticksPerMs;
    private final long dataTimeStart;
    private long latestDataTime;

    private long processCounter = 0;
    private StopWatch processWatch;

    private long currentSequence = 0;

    /**
     *
     * @param producer producer to write to
     * @param systemTimeStart system time to start producing
     * @param ticksPerMs data ticks (ms) per system ms
     * @param dataTimeStart start time of the dataset
     */
    public SystemTimeSynchronizer(IProducer producer, long systemTimeStart, long ticksPerMs, long dataTimeStart){
        this.producer = producer;
        this.systemTimeStart = systemTimeStart;
        this.ticksPerMs = ticksPerMs;
        this.dataTimeStart = dataTimeStart;

    }

    @Override
    public void onEvent(IDataEntry data, long sequence, boolean endOfBatch) throws Exception {
        this.currentSequence = sequence;
        Long currentSystemTime = System.currentTimeMillis();
        if(systemTimeStart > currentSystemTime){
            try {
                long waitTime = systemTimeStart - currentSystemTime;
                logger.debug("waiting {} ms to system start", waitTime);
                Thread.sleep(waitTime);
                currentSystemTime = System.currentTimeMillis();
            } catch (InterruptedException e) {
                logger.error("could not wait for start time", e);
            }
        }

        // we do not want to recalculate in case the data timestamp stays the same. Since this happens often with
        // this data, we skip this test to save time.
        if(latestDataTime != data.getTimestamp()) {
            long diff = currentSystemTime - systemTimeStart;
            long diffRel = diff * ticksPerMs;
            long currentDataTime = dataTimeStart + diffRel;

            if(data.getTimestamp() > currentDataTime){
                try {
                    long waitTime = (data.getTimestamp() - currentDataTime)/ticksPerMs;
                    logger.debug("waiting for data time to synchronize; wait time: {};  current relative time: {}, data time: {}", waitTime, currentDataTime, data.getTimestamp());
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                    logger.error("could not wait for data time to align with system time", e);
                }
            } else{
                long lag = data.getTimestamp() - currentDataTime;
                //logger.info(performance, "topic={} lag={} currentSystemTime={} currentDataTime={}", lagTopic, lag, currentSystemTime, data.getTimestamp());
            }
        }

        this.producer.send(data);
        latestDataTime = data.getTimestamp();


        if(this.processCounter == 0){
            this.processWatch = new StopWatch();
        }
        this.processCounter++;
        if(this.processCounter % 1000 == 0){
            this.processWatch.stop();
            logger.info(performance, "topic=synchronizerThroughput stepSize=1000 duration={}", this.processWatch.getTimeMicros());
            this.processWatch = new StopWatch();
        }
    }

    public long getCurrentSequence() {
        return currentSequence;
    }
}
