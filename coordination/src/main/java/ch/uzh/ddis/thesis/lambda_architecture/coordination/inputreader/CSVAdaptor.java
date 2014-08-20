package ch.uzh.ddis.thesis.lambda_architecture.coordination.inputreader;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.data.IDataFactory;
import com.ecyrd.speed4j.StopWatch;
import com.lmax.disruptor.RingBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.io.*;

/**
 * Reads messages from a CSV file and generates data entries that are then send to the system time synchronizer.
 * There are other implementations for CSV file based readers, but frankly they usually encapsulate a lot of overhead.
 * Also the CSVReader does not have any knowledge about the entries other than it is a IDataEntry like object. The parsing
 * is delegated to the data factory.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class CSVAdaptor implements Runnable{
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");
    private static final String performanceTopicThroughput = "csvreaderthroughput";
    private static final String performanceTopicTotal = "csvreadertotal";

    private final FileReader csvFileReader;
    private final RingBuffer<IDataEntry> buffer;
    private final IDataFactory dataFactory;
    private final String csvName;

    private boolean finished = false;

    /**
     *
     * @param csv csv file to read messages from
     * @param buffer buffer to produce the messages
     * @param dataFactory data factory to parse the message read from the csv
     * @throws FileNotFoundException thrown if the provided csv file does not exist.
     */
    public CSVAdaptor(File csv, RingBuffer<IDataEntry> buffer, IDataFactory dataFactory) throws FileNotFoundException {
        this.buffer = buffer;
        this.csvFileReader = new FileReader(csv);
        this.dataFactory = dataFactory;
        this.csvName = csv.getName();
    }

    /**
     * Reads the csv line by line, creates IDataEntries and sends these entries to the buffer.
     */
    public void produceMessages(){
        StopWatch watch = new StopWatch(performanceTopicThroughput);
        BufferedReader reader = new BufferedReader(csvFileReader);
        try {
            String line;
            int performanceCounter = 0;
            while ((line = reader.readLine()) != null) {
                long sequence = buffer.next();

                try
                {
                    IDataEntry data = buffer.get(sequence);
                    data.init(line);
                } finally {
                    buffer.publish(sequence);
                }

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

        try {
            reader.close();
        } catch (IOException e) {
            logger.error(e);
        }
    }

    @Override
    public void run() {
        StopWatch watch = new StopWatch(performanceTopicTotal);
        this.produceMessages();
        watch.stop();
        logger.info(performance, "topic={} duration={} csv={}", performanceTopicTotal, watch.getTimeMicros(), csvName);

        this.finished = true;
    }


    public boolean isFinished() {
        return finished;
    }
}
