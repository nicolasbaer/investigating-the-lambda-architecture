package ch.uzh.ddis.thesis.lambda_architecture.speed.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.ecyrd.speed4j.StopWatch;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.net.UnknownHostException;
import java.util.Map;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class ResultBolt extends BaseRichBolt {
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");

    private final String mongoDbHost;
    private final String mongoDbPort;
    private final String mongoDbName;

    private MongoClient mongoClient;
    private DB db;

    long processCounter = 0;
    StopWatch processWatch;

    public ResultBolt(final String mongoDbHost, final String mongoDbPort, final String mongoDbName) {
        this.mongoDbHost = mongoDbHost;
        this.mongoDbPort = mongoDbPort;
        this.mongoDbName = mongoDbName;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            mongoClient = new MongoClient(mongoDbHost, Integer.valueOf(mongoDbPort));
            this.db = mongoClient.getDB(mongoDbName);
        } catch (UnknownHostException e){
            logger.error(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        Map<String, Object> result = (Map<String, Object>) input.getValueByField("result");
        String topic = input.getStringByField("topic");

        DBCollection collection = this.db.getCollection("result");
        BasicDBObject doc = new BasicDBObject(result);
        collection.save(doc);

        if(processCounter == 0){
            this.processWatch = new StopWatch();
        }

        if(processCounter % 1000 == 0) {
            this.processWatch.stop();
            logger.info(performance, "topic=speedStoreResult resultStream={} stepSize=1000 duration={}", topic, this.processWatch.getTimeMicros());
            this.processWatch = new StopWatch();
        }
        processCounter++;

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // no output
    }
}
