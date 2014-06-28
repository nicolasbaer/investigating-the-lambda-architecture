package ch.uzh.ddis.thesis.lambda_architecture.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import ch.uzh.ddis.thesis.lambda_architecture.data.SRBench.SRBenchDataFactory;
import ch.uzh.ddis.thesis.lambda_architecture.storm.bolt.ResultBolt;
import ch.uzh.ddis.thesis.lambda_architecture.storm.bolt.SRBench.SRBenchQ1Bolt;
import ch.uzh.ddis.thesis.lambda_architecture.storm.spout.NettySpout;
import com.google.common.net.HostAndPort;

import java.util.ArrayList;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class SRBenchQ1Topology {


    public static void main(String[] args) {

        String topologyServer = null;


        ArrayList<HostAndPort> hosts = new ArrayList<>(8);
        hosts.add(HostAndPort.fromString("127.0.0.1:5050"));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("netty_spout", new NettySpout(hosts, new SRBenchDataFactory()), 1);
        builder.setBolt("srbench-q1", new SRBenchQ1Bolt("localhost"), 1).fieldsGrouping("netty_spout", new Fields("partition"));
        builder.setBolt("store_result", new ResultBolt(), 2).shuffleGrouping("srbench-q1");

        Config conf = new Config();
        conf.setDebug(true);

        if (topologyServer != null) {
            conf.setNumWorkers(1);

            try {
                StormSubmitter.submitTopology(topologyServer, conf, builder.createTopology());
            } catch (InvalidTopologyException | AlreadyAliveException e) {
                e.printStackTrace();
            }
        }
        else {
            LocalCluster cluster = new LocalCluster();
            System.out.println("submitting topology");
            cluster.submitTopology("srbench-q1-test", conf, builder.createTopology());
            System.out.println("submitted topology");
            Utils.sleep(100000);
            cluster.killTopology("srbench-q1-test");
            cluster.shutdown();
        }
    }


}
