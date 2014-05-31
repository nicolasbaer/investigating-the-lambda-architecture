package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.UUID;

/**
 * At it's best a simple wrapper around the kafka API.
 *
 * @param <E> Type of partitioning key
 * @param <F> Type of message
 */
public class KafkaProducer<F extends IDataEntry> {

    private final Producer<String, F> producer;
    private String topicPrefix = "";
    private boolean dataTopic = false;

    public KafkaProducer(Properties properties) {
        properties.setProperty("client.id", topicPrefix + UUID.randomUUID());
        ProducerConfig config = new ProducerConfig(properties);
        this.producer = new Producer<>(config);
    }

    public KafkaProducer(Properties properties, String topicPrefix, boolean dataTopic) {
        this(properties);
        this.topicPrefix = topicPrefix;
        this.dataTopic = dataTopic;
    }

    /**
     * Sends the message to kafka
     *
     * @param topic topic name, if the producer was given a prefix, the prefix will be prepended
     * @param message message to deliver to kafka
     * @param key the message key to decide the partitioning scheme
     */
    public void send(final F message){
        StringBuilder topic = new StringBuilder().append(this.topicPrefix);
        if(this.dataTopic){
            topic.append(message.getTopic());
        }

        KeyedMessage<String, F> keyedMessage = new KeyedMessage<>(topic.toString(), message.getPartitionKey(), message);
        producer.send(keyedMessage);
    }
}
