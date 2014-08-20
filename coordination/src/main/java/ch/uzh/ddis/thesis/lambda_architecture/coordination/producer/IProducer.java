package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;

/**
 * The producer interface defines the behavior to produce messages to other layers.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public interface IProducer {

    /**
     * Opens a connection to the destination if any.
     */
    public void open();

    /**
     * Sends messages to the external system.
     * @param message message to send
     */
    public void send(final IDataEntry message);

    /**
     * closes the all connections.
     */
    public void close();
}
