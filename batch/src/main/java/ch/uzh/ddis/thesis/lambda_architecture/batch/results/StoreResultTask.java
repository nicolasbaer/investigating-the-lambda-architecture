package ch.uzh.ddis.thesis.lambda_architecture.batch.results;

import com.ecyrd.speed4j.StopWatch;
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
 * Samza task to store results to the service layer (e.g. mongodb). This task is designed to work with every data set
 * and every question. It fetches the result from the configured Kafka topic (has to be a map), writes the result
 * to MongoDB and checkpoints after each result is stored.
 *
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

    private boolean indexCreated = false;

    private StopWatch processWatch;

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
        DBCollection collection = this.db.getCollection("result");
        Map<String, Object> result = (Map<String, Object>) incomingMessageEnvelope.getMessage();

        if(!indexCreated){
            indexCreated = true;

            BasicDBObject index = new BasicDBObject();
            for(String key : result.keySet()){
                index.append(key, 1);
            }

            collection.createIndex(index);
        }

        String stream = incomingMessageEnvelope.getSystemStreamPartition().getStream();
        BasicDBObject doc = new BasicDBObject(result);
        collection.save(doc);

        taskCoordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);

        if(this.processCounter == 0){
            this.processWatch = new StopWatch();
        }

        processCounter++;
        if(processCounter % 100 == 0) {
            this.processWatch.stop();
            logger.info(performance, "topic=samzastoreresult stream={} stepSize=100 duration={}", stream, this.processWatch.getTimeMicros());
            this.processWatch = new StopWatch();
        }

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
