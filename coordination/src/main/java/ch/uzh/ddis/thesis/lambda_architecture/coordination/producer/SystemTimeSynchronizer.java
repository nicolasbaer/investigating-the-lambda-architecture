package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;
import com.google.common.base.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Synchronizes the production rate of data entries with the system time.
 * A sensor can send entries to this object in order to pipe it to Kafka with respect to a certain sending rate, which
 * is bound to the system time.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class SystemTimeSynchronizer<E extends IDataEntry> implements Runnable {
    private static final Logger logger = LogManager.getLogger();

    private static final int MAX_QUEUE_SIZE = 1000;

    private final KafkaProducer<E> producer;
    private final long systemTimeStart;
    private final long ticksPerMs;
    private final ArrayList<ArrayBlockingQueue<Pair<E, Integer>>> queues;

    private ArrayList<Boolean> removeQueue;


    public SystemTimeSynchronizer(KafkaProducer<E> producer, long systemTimeStart, long ticksPerMs){
        this.producer = producer;
        this.systemTimeStart = systemTimeStart;
        this.ticksPerMs = ticksPerMs;

        this.queues = new ArrayList<>();
        this.removeQueue = new ArrayList<>();
    }

    /**
     * Registers a new data input and initializes the corresponding queue to write data to.
     * @return queue number to add data into
     */
    public synchronized int registerDataInput(){
        queues.add(new ArrayBlockingQueue<Pair<E, Integer>>(MAX_QUEUE_SIZE));
        removeQueue.add(false);
        return (queues.size() - 1);
    }

    /**
     * Removes a registered data input queue.
     * @param queueId id of the queue to remove
     */
    public void removeDataInput(int queueId){
        this.removeQueue.set(queueId, true);
    }

    /**
     * Add a data entry to be processed.
     * @param queueId queue number to add data to
     * @param data data to add
     */
    public void addData(int queueId, E data){
        logger.debug("received new data for queue {}", queueId);
        try {
            this.queues.get(queueId).put(new Pair<>(data, queueId));
        } catch (InterruptedException e){
            logger.error("waiting for queue to append data was interrupted.");
        }
    }

    /**
     * Gets the first timestamp from the elements in the queues.
     * This method relies on the queue being in timely order.
     * @return timestamp of first entry (ordered by date)
     */
    private long getFirstTimestamp(){
        long timestamp = -1;
        for(ArrayBlockingQueue<Pair<E, Integer>> queue : queues){
            Optional<Pair<E, Integer>> optionalElement;
            do {
                optionalElement = Optional.fromNullable(queue.peek());
                logger.debug("waiting for first element in queue");
            }while(!optionalElement.isPresent());

            Pair<E, Integer> dataPair = optionalElement.get();
            E data = dataPair.getValue0();
            if(data.getTimestamp() < timestamp || timestamp == -1){
                timestamp = data.getTimestamp();
            }

        }

        logger.debug("found first timestamp: {}", timestamp);

        return timestamp;
    }

    /**
     * Processes the incoming data of the queue and hands it over to the kafka producer.
     * This method takes care of system time synchronization and makes sure events are sent in order.
     *
     * @param firstTimestamp first timestamp of the data
     * @param systemTimeStart system time start
     * @param ticksPerMs the amount of ticks per milliseconds
     *                   (e.g. 1000 would mean every millisecond goes one second in data to kafka.)
     */
    private void processQueues(long firstTimestamp, long systemTimeStart, long ticksPerMs){
        PriorityQueue<Pair<E, Integer>> toProcess = new PriorityQueue<>(this.queues.size(), new Comparator<Pair<E, Integer>>() {
            @Override
            public int compare(Pair<E, Integer> o1, Pair<E, Integer> o2) {
                E data1 = o1.getValue0();
                E data2 = o2.getValue0();
                return Long.valueOf(data1.getTimestamp()).compareTo(data2.getTimestamp());
            }
        });

        for(int i = 0; i < this.queues.size(); i++){
            try {
                Pair<E, Integer> dataPair = this.queues.get(i).take();
                toProcess.add(dataPair);
            } catch (InterruptedException e) {
                logger.error("could not take element from queue {}", e, i);
            }
        }

        long latestDataTime = 0;
        while(!toProcess.isEmpty()){
            Pair<E, Integer> dataPair = toProcess.poll();
            E data = dataPair.getValue0();
            Integer queueId = dataPair.getValue1();

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
                long currentDataTime = firstTimestamp + diffRel;

                if(data.getTimestamp() > currentDataTime){
                    try {
                        long waitTime = (data.getTimestamp() - currentDataTime)/ticksPerMs;
                        logger.debug("waiting for data time to synchronize; wait time: {};  current relative time: {}, data time: {}", waitTime, currentDataTime, data.getTimestamp());
                        Thread.sleep(waitTime);
                    } catch (InterruptedException e) {
                        logger.error("could not wait for data time to align with system time", e);
                    }
                }
            }

            this.producer.send(data);
            latestDataTime = data.getTimestamp();

            if(this.queues.get(queueId).isEmpty() && this.removeQueue.get(queueId)){
                this.queues.set(queueId, null);
            } else{
                try {
                    toProcess.add(this.queues.get(queueId).take());
                }catch (InterruptedException e){
                    logger.error("could not read next element from queue {}", e, queueId);
                }
            }
        }
    }


    @Override
    public void run() {
        long firstTimestamp = this.getFirstTimestamp();
        this.processQueues(firstTimestamp, this.systemTimeStart, this.ticksPerMs);
    }

}
