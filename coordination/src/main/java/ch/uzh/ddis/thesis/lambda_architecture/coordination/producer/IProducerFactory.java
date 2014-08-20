package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

/**
 * The producer factory abstracts the logic to create a new producer.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public interface IProducerFactory {

    public IProducer makeProducer();

}
