package ch.uzh.ddis.thesis.lambda_architecture.kafka;

import ch.uzh.ddis.thesis.lambda_architecture.kafka.producer.CSVAdaptorPerformanceTest;
import com.ecyrd.speed4j.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class ProducerPerformanceCSV {
    private final static Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");

    public static void main(String[] args) {
        try {
            Properties props = new Properties();
            props.put("metadata.broker.list", "localhost:9092");
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("key.serializer.class", "kafka.serializer.StringEncoder");
            props.put("producer.type", "sync"); // use sync to not break the order of events

            int batchSize = 1;
            if(args.length > 0) {
                batchSize = new Integer(args[0]);
            }

            final URI uri = ProducerPerformanceCSV.class.getClassLoader().getResource("data/debs/first30k.csv").toURI();
            final File csv = new File(uri);
            boolean first = true;
            while(batchSize <= 1000){
                logger.info("running performance test for batch size {}", batchSize);
                StopWatch watchBatchSize = new StopWatch("kafka_csv_batchsize_json");

                ExecutorService executor = Executors.newFixedThreadPool(1);
                Runnable producerThread = new CSVAdaptorPerformanceTest("csv-test", csv, props, batchSize);
                executor.execute(producerThread);

                executor.shutdown();
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

                watchBatchSize.stop();
                logger.info(performance, "topic={} batchSize={} duration={}", watchBatchSize.getTag(), batchSize, watchBatchSize.getTimeMicros());

                if(first){
                    batchSize += 9;
                } else{
                    batchSize += 10;
                }
                first = false;
            }

        } catch (URISyntaxException ex){
            logger.error("could not load csv file! {}", ex.getMessage());
        } catch(InterruptedException ex){
            logger.error("producer thread crashed unexpectedly: {}", ex.getMessage());
        }
    }


}
