package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.UUID;

/**
 * At it's best a simple wrapper around the kafka API.
 */
public class KafkaProducer implements IProducer{
    private static final Logger logger = LogManager.getLogger();

    private final Producer<String, String> producer;
    private String topic = "";

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
        producer.send(keyedMessage);
    }

    @Override
    public void open() {
        // no-op
    }

    @Override
    public void close() {
        this.producer.close();
    }
}
