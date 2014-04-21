package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * At it's best a simple wrapper around the kafka API.
 * This implementation will overwrite the properties in order to make the producer async.
 * If no batch size is given, it will set the default to 200.
 *
 * @param <E> Type of partitioning key
 * @param <F> Type of message
 */
public class KafkaProducer<E, F> {

    private final Producer<E, F> producer;
    private String topicPrefix = "";

    public KafkaProducer(Properties properties) {
        properties = this.overwriteProperties(properties);
        ProducerConfig config = new ProducerConfig(properties);
        this.producer = new Producer<>(config);
    }

    public KafkaProducer(Properties properties, String topicPrefix) {
        this(properties);
        this.topicPrefix = topicPrefix;
    }

    /**
     * Sends the message to kafka
     *
     * @param topic topic name, if the producer was given a prefix, the prefix will be prepended
     * @param message message to deliver to kafka
     * @param key the message key to decide the partitioning scheme
     */
    public void send(final String topic, final E key, final F message){
        String prefixedTopic = topicPrefix + topic;
        KeyedMessage<E, F> keyedMessage = new KeyedMessage<>(prefixedTopic, key, message);
        producer.send(keyedMessage);
    }


    private Properties overwriteProperties(Properties properties){
        properties.put("producer.type", "async");
        properties.put("batch.size", "200");

        return properties;
    }
}
