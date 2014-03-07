package ch.uzh.ddis.thesis.lambda_architecture.kafka.producer;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.message.MultiformatMessage;
import org.apache.logging.log4j.message.SimpleMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * Simple Producer example that reads data from a CSV file.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class ProducerPerformanceTest implements Runnable{
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");
    private static final int ROUNDS = 50000;

    private final kafka.javaapi.producer.Producer<String, String> producer;
    private final String topic;
    private final String stringToPublish;
    private final int batchSize;

    /**
     * default constructor
     */
    public ProducerPerformanceTest(final String topic, final String stringToPublish, final Properties props, final int batchSize) {
        producer = new kafka.javaapi.producer.Producer<>(new ProducerConfig(props));
        this.topic = topic;
        this.stringToPublish = stringToPublish;
        this.batchSize = batchSize;
    }

    /**
     * Pushes the given string to the kafka queue.
     */
    private void publishString(){
        List<KeyedMessage<String, String>> messages = new ArrayList<>();
        for (int i = 0; i < ROUNDS; i++){
            KeyedMessage<String, String> message = new KeyedMessage<>(this.topic, "test-partition", this.stringToPublish);
            messages.add(message);

            if(i % (this.batchSize) == 0d){
                this.producer.send(messages);
                messages.clear();

                if(i % 1000 == 0){
                    final int finalI = i;
                    logger.info(performance, "\"test\": \"test\"");
                    //logger.info(performance, new MapMessage(){{ put("test", "test");}});
                    //logger.info(performance, new JsonMessage("{\"test\":\"test2\"}"));
                    //logger.info(performance, "batchSize={}, counter={}", this.batchSize, i);
                }
            }
        }
    }


    public void run(){
        this.publishString();
    }


    protected class JsonMessage extends SimpleMessage implements MultiformatMessage{
        public JsonMessage(final String message) {
            super(message);
        }
        @Override
        public String getFormat() {
            return "JSON";
        }

        @Override
        public String getFormattedMessage(String[] formats) {
            return super.getFormattedMessage();
        }

        @Override
        public String[] getFormats() {
            return new String[]{"JSON"};
        }
    }

}
