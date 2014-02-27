package ch.uzh.ddis.thesis.lambda_architecture.kafka;

import ch.uzh.ddis.thesis.lambda_architecture.kafka.producer.CSVAdaptor;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class Main {
    private final static Logger LOGGER = Logger.getLogger(CSVAdaptor.class.getName());



    public static void main(String[] args) {
        try {
            Properties props = new Properties();
            props.put("metadata.broker.list", "localhost:9092");
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("key.serializer.class", "kafka.serializer.StringEncoder");
            props.put("zk.connect", "localhost:2181");
            props.put("producer.type", "sync"); // use sync to not break the order of events

            URI uri = Main.class.getClassLoader().getResource("data/debs/first30k.csv").toURI();
            File csv = new File(uri);

            ExecutorService executor = Executors.newFixedThreadPool(1);
            Runnable producerThread = new CSVAdaptor("debs-data", csv, props);
            executor.execute(producerThread);

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        } catch (URISyntaxException ex){
            LOGGER.log(Level.SEVERE, "could not load csv file! " + ex.getMessage());
        } catch(InterruptedException ex){
            LOGGER.log(Level.SEVERE, "producer thread crashed unexpectedly: " + ex.getMessage());
        }



    }

}
