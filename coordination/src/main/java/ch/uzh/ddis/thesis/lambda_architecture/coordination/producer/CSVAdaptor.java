package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.data.IDataFactory;
import com.ecyrd.speed4j.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.io.*;

/**
 *
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class CSVAdaptor<E extends IDataEntry> implements Runnable{
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");
    private static final String performanceTopicThroughput = "csvreaderthroughput";
    private static final String performanceTopicTotal = "csvreadertotal";

    private final FileReader csvFileReader;
    private final SystemTimeSynchronizer<E> synchronizer;
    private final IDataFactory<E> dataFactory;
    private final String csvName;
    private final int queueId;

    /**
     * @param csv CSV File to read from
     * @param synchronizer time synchronizer to append data to.
     */
    public CSVAdaptor(File csv, SystemTimeSynchronizer<E> synchronizer, IDataFactory<E> dataFactory, int queueId) throws FileNotFoundException {
        this.synchronizer = synchronizer;
        this.csvFileReader = new FileReader(csv);
        this.dataFactory = dataFactory;
        this.csvName = csv.getName();
        this.queueId = queueId;
    }

    /**
     * produces all messages from the csv (line by line) with the given producer.
     */
    public void produceMessages(){
        StopWatch watch = new StopWatch(performanceTopicThroughput);
        BufferedReader reader = new BufferedReader(csvFileReader);
        try {
            String line;
            int performanceCounter = 0;
            while ((line = reader.readLine()) != null) {
                synchronizer.addData(queueId, dataFactory.makeDataEntryFromCSV(line));

                if(performanceCounter % 1000 == 0){
                    watch.stop();
                    logger.info(performance, "topic={} step={} stepSize={} duration={} csv={}", watch.getTag(), 1000,
                            performanceCounter, watch.getTimeMicros(), csvName);
                    watch = new StopWatch(performanceTopicThroughput);
                }

                performanceCounter++;
            }
        } catch (IOException e){
            logger.error("could not read csv line with error message `{}`", e.getMessage());
        }
    }

    @Override
    public void run() {
        StopWatch watch = new StopWatch(performanceTopicTotal);
        this.produceMessages();
        watch.stop();
        logger.info(performance, "topic={} duration={} csv={}", performanceTopicTotal, watch.getTimeMicros(), csvName);
    }
}
