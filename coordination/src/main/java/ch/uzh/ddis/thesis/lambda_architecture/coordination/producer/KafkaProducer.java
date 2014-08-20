package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;
import com.ecyrd.speed4j.StopWatch;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * At it's best a simple wrapper around the kafka API :)
 */
public class KafkaProducer implements IProducer{
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");

    private static final int bufferSize = 200;

    private final Producer<String, String> producer;
    private String topic = "";

    private List<KeyedMessage<String, String>> messageBuffer  = new ArrayList<>(bufferSize);

    private long processCounter = 0;
    private StopWatch processWatch;

    public KafkaProducer(Properties properties) {
        properties.setProperty("client.id", topic + UUID.randomUUID());
        ProducerConfig config = new ProducerConfig(properties);
        this.producer = new Producer<>(config);
    }

    public KafkaProducer(Properties properties, String topic) {
        this(properties);
        this.topic = topic;
    }

    /**
     * Sends the message to kafka
     *
     * @param message message to deliver to kafka
     */
    public void send(final IDataEntry message){
        KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(topic, message.getPartitionKey(), message.toString());

        this.producer.send(keyedMessage);

        if(this.processCounter == 0){
            this.processWatch = new StopWatch();
        }
        this.processCounter++;
        if(this.processCounter % 1000 == 0){
            this.processWatch.stop();
            logger.info(performance, "topic=kafkaProducerThroughput stepSize=1000 duration={}", this.processWatch.getTimeMicros());
            this.processWatch = new StopWatch();
        }
    }

    @Override
    public void open() {
        // no-op
    }

    @Override
    public void close() {

        if(!this.messageBuffer.isEmpty()){
            this.producer.send(this.messageBuffer);
        }

        this.producer.close();
    }
}
