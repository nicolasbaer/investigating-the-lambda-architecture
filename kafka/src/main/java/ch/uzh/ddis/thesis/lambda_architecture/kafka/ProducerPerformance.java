package ch.uzh.ddis.thesis.lambda_architecture.kafka;

import ch.uzh.ddis.thesis.lambda_architecture.kafka.producer.ProducerPerformanceTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class ProducerPerformance {
    private final static Logger logger = LogManager.getLogger(ProducerPerformance.class.getName());


    public static void main(String[] args) {
        try {
            Properties props = new Properties();
            props.put("metadata.broker.list", "localhost:9092");
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("key.serializer.class", "kafka.serializer.StringEncoder");
            props.put("zk.connect", "localhost:2181");
            props.put("producer.type", "sync"); // use sync to not break the order of events

            int batchSize = 1;
            if(args.length > 0) {
                batchSize = new Integer(args[0]);
            }

            logger.info("testing with batch size {}", batchSize);


            ExecutorService executor = Executors.newFixedThreadPool(1);
            Runnable producerThread = new ProducerPerformanceTest("performance-test-string", "1000000000000,1377986401,68.451,0,11,0,0", props, batchSize);
            executor.execute(producerThread);

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

            logger.info("test done.");

        } catch(InterruptedException ex){
            logger.error("producer thread crashed unexpectedly: %s", ex.getMessage());
        }
    }


}
