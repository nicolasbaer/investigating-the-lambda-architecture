package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

import java.util.Properties;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class KafkaProducerFactory implements IProducerFactory {

    private final Properties properties;
    private final String topic;


    public KafkaProducerFactory(Properties properties, String topic) {
        this.properties = properties;
        this.topic = topic;
    }


    @Override
    public IProducer makeProducer() {
       return new KafkaProducer(properties, this.topic);
    }
}
