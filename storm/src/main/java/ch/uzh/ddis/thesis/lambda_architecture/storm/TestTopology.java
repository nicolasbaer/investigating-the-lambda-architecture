package ch.uzh.ddis.thesis.lambda_architecture.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class TestTopology {
    private final static Logger LOGGER = Logger.getLogger(TestTopology.class.getName());

    protected static class TestSpout extends BaseRichSpout{
        private SpoutOutputCollector collector = null;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(100);
            final String[] words = new String[] {"test1", "test2", "test3", "test4"};
            final Random rand = new Random();
            final String word = words[rand.nextInt(words.length)];
            this.collector.emit(new Values(word));
        }
    }

    protected static class TestBolt extends BaseRichBolt{
        private OutputCollector collector = null;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            String word = (String) input.getValueByField("word");
            LOGGER.log(Level.INFO, "got word " + word);
            word = word + "-passed bolt";
            this.collector.emit(new Values(word));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }


    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("mywords", new TestSpout(), 10);
        builder.setBolt("altered", new TestBolt(), 3).shuffleGrouping("mywords");
        builder.setBolt("altered2", new TestBolt(), 2).shuffleGrouping("altered");

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
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }


    }


}
