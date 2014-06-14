package ch.uzh.ddis.thesis.lambda_architecture.batch.results;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;

import java.util.Map;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class StoreResultTask implements InitableTask, WindowableTask, StreamTask {
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");

    private static final long shutdownWaitThreshold = (1000 * 60 * 30); // 30 minutes

    private MongoClient mongoClient;
    private DB db;

    long lastResultReceived = 0;
    long processCounter = 0;

    @Override
    public void init(Config config, TaskContext taskContext) throws Exception {
        String mongoDbHost = config.get("custom.result.mongodb.host");
        String mongoDbPort = config.get("custom.result.mongodb.port");
        String mongoDbName = config.get("custom.result.mongodb.db");

        mongoClient = new MongoClient(mongoDbHost, Integer.valueOf(mongoDbPort));
        this.db = mongoClient.getDB(mongoDbName);
    }

    @Override
    public void process(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        String stream = incomingMessageEnvelope.getSystemStreamPartition().getStream();
        DBCollection collection = this.db.getCollection(stream);

        Map<String, Object> result = (Map<String, Object>) incomingMessageEnvelope.getMessage();
        BasicDBObject doc = new BasicDBObject(result);
        collection.save(doc);

        if(processCounter % 1000 == 0) {
            logger.info(performance, "topic=samzastoreresult stream={} stepSize=100", stream);
        }
        processCounter++;

        this.lastResultReceived = System.currentTimeMillis();
    }

    @Override
    public void window(MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        if(lastResultReceived == 0)
            return;
        long currentTime = System.currentTimeMillis();
        if((currentTime - this.lastResultReceived) >  shutdownWaitThreshold){
            taskCoordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
        }
    }
}
