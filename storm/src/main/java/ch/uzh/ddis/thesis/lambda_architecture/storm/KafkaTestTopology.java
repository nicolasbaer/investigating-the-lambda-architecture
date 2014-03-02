package ch.uzh.ddis.thesis.lambda_architecture.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class KafkaTestTopology {
    private static Logger LOGGER = Logger.getLogger(KafkaTestTopology.class.getName());

    protected static class DebsTestBolt extends BaseRichBolt{

        private OutputCollector collector = null;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
           this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            LOGGER.log(Level.INFO, "GOT INPUT: " + input.toString());
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("debs"));
        }
    }


    public static void main(String[] args) {

        ZkHosts brokerHosts = new ZkHosts("localhost:2181");
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "debs-data", "", "storm-test1");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("debs-data", new KafkaSpout(kafkaConfig), 10);
        builder.setBolt("print", new DebsTestBolt()).shuffleGrouping("debs-data");


        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (InvalidTopologyException | AlreadyAliveException e) {
                e.printStackTrace();
            }
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000000);
            cluster.killTopology("test");
            cluster.shutdown();
        }

    }
}
