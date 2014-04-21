package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

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
public class CSVAdaptor implements Runnable{
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");
    private static final String performanceTopicThroughput = "kafka-producer-throughput";
    private static final String performanceTopicTotal = "kafka-producer-total";

    private final static String delimiter = ",";

    private final FileReader csvFileReader;
    private final int keyIndex;
    private final int topicIndex;
    private final KafkaProducer<String, String> producer;
    private final String csvName;

    /**
     * @param csv CSV File to read from
     * @param keyIndex index of the key field, the index starts at 0
     * @param producer kafka producer to send messages
     */
    public CSVAdaptor(File csv, int keyIndex, int topicIndex, KafkaProducer<String, String> producer)
            throws FileNotFoundException {
        this.keyIndex = keyIndex;
        this.producer = producer;
        this.csvFileReader = new FileReader(csv);
        this.topicIndex = topicIndex;
        this.csvName = csv.getName();
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
                String[] splitLine = line.split(delimiter);
                String key = splitLine[keyIndex];
                String topic = splitLine[topicIndex];

                producer.send(topic, key, line);

                if(performanceCounter % 1000 == 0){
                    watch.stop();
                    logger.info(performance, "topic={} step={} stepSize={} duration={} csv={}", watch.getTag(), 1000,
                            performanceCounter, watch.getTimeMicros(), csvName);
                    watch = new StopWatch(performanceTopicThroughput);
                }
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
