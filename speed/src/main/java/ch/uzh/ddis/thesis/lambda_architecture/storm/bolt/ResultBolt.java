package ch.uzh.ddis.thesis.lambda_architecture.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class ResultBolt extends BaseRichBolt {

    public ResultBolt() {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        Map<String, Object> result = (Map<String, Object>) input.getValueByField("result");
        String topic = input.getStringByField("topic");


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // no output
    }
}
