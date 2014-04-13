package ch.uzh.ddis.thesis.lambda_architecture.samza.task;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.util.logging.Logger;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class DebsReadTask implements StreamTask{

    private final static Logger logger = Logger.getLogger(DebsReadTask.class.getName());

    @Override
    public void process(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {

        logger.info("received message:" + incomingMessageEnvelope.getMessage());

        // just write the message to another topic at the moment..
        messageCollector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "debs-out"), incomingMessageEnvelope.getMessage()));

    }
}
