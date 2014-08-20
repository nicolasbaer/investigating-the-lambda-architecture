package ch.uzh.ddis.thesis.lambda_architecture.speed.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import ch.uzh.ddis.thesis.lambda_architecture.data.Dataset;
import ch.uzh.ddis.thesis.lambda_architecture.speed.bolt.ResultBolt;
import ch.uzh.ddis.thesis.lambda_architecture.speed.bolt.SRBench.*;
import ch.uzh.ddis.thesis.lambda_architecture.speed.bolt.debs.DebsQ1HouseBolt;
import ch.uzh.ddis.thesis.lambda_architecture.speed.bolt.debs.DebsQ3HouseBolt;
import ch.uzh.ddis.thesis.lambda_architecture.speed.grouping.PartitionGrouping;
import ch.uzh.ddis.thesis.lambda_architecture.speed.spout.NettySpout;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.net.HostAndPort;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Properties;

/**
 *
 *
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class TopologyHelper {

    @Parameter(names = "-question", description = "question to solve e.g. `srbench-q1`, `debs-q1-1min-plug`", required = true)
    public String question;

    @Parameter(names = "-dataset", description = "dataset to work on `srbench` or `debs`", required = true)
    public String dataset;

    /**
     * Starts the topology based on the given cli arguments and properties in the property file.
     * The following highlights the main points to consider:
     * - A spout is started for each netty host provided in the properties file in order to consume data.
     * - The spout will partition the
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws AlreadyAliveException
     * @throws InvalidTopologyException
     */
    public void start() throws IOException, URISyntaxException, AlreadyAliveException, InvalidTopologyException {
        Properties properties = new Properties();
        properties.load(this.getClass().getResourceAsStream("/speed.properties"));

        String nettyServerList = properties.getProperty("speed.netty.server.list");
        int numWorkers = Integer.valueOf(properties.getProperty("speed.num.workers"));
        int partitions = Integer.valueOf(properties.getProperty("speed.num.partitions"));
        String redisHost = properties.getProperty("speed.redis.host");
        String mongoDbHost = properties.getProperty("speed.result.mongodb.host");
        String mongoDbPort = properties.getProperty("speed.result.mongodb.port");
        String mongoDbName = properties.getProperty("speed.result.mongodb.db");

        ArrayList<HostAndPort> hosts = this.parseHosts(nettyServerList);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("netty", new NettySpout(hosts, Dataset.valueOf(this.dataset).getFactory()), hosts.size());

        if(this.question.equals("srbench-q1")){
            builder.setBolt(this.question, new SRBenchQ1Bolt(redisHost), partitions).customGrouping("netty", new PartitionGrouping());
        } else if(this.question.equals("srbench-q2")){
            builder.setBolt(this.question, new SRBenchQ2Bolt(redisHost), partitions).customGrouping("netty", new PartitionGrouping());
        } else if(this.question.equals("srbench-q3")){
            builder.setBolt(this.question, new SRBenchQ3Bolt(redisHost), partitions).customGrouping("netty", new PartitionGrouping());
        } else if(this.question.equals("srbench-q4")){
            builder.setBolt(this.question, new SRBenchQ4Bolt(redisHost), partitions).customGrouping("netty", new PartitionGrouping());
        } else if(this.question.equals("srbench-q5")){
            builder.setBolt(this.question, new SRBenchQ5Bolt(redisHost), partitions).customGrouping("netty", new PartitionGrouping());
        } else if(this.question.equals("srbench-q6")){
            builder.setBolt(this.question, new SRBenchQ6Bolt(redisHost), partitions).customGrouping("netty", new PartitionGrouping());
        } else if(this.question.equals("srbench-q7")) {
            builder.setBolt(this.question, new SRBenchQ7Bolt(redisHost), partitions).customGrouping("netty", new PartitionGrouping());
        } else if(this.question.equals("debs-q1-house-1min")){
            builder.setBolt(this.question, new DebsQ1HouseBolt(redisHost, 1l), partitions).customGrouping("netty", new PartitionGrouping());
        }  else if(this.question.equals("debs-q1-house-5min")){
            builder.setBolt(this.question, new DebsQ1HouseBolt(redisHost, 5l), partitions).customGrouping("netty", new PartitionGrouping());
        }  else if(this.question.equals("debs-q1-house-15min")){
            builder.setBolt(this.question, new DebsQ1HouseBolt(redisHost, 15l), partitions).customGrouping("netty", new PartitionGrouping());
        }  else if(this.question.equals("debs-q1-house-60min")){
            builder.setBolt(this.question, new DebsQ1HouseBolt(redisHost, 60l), partitions).customGrouping("netty", new PartitionGrouping());
        }  else if(this.question.equals("debs-q1-house-120min")){
            builder.setBolt(this.question, new DebsQ1HouseBolt(redisHost, 120l), partitions).customGrouping("netty", new PartitionGrouping());
        } else if(this.question.equals("debs-q1-plug-1min")){
            builder.setBolt(this.question, new DebsQ1HouseBolt(redisHost, 1l), partitions).customGrouping("netty", new PartitionGrouping());
        }  else if(this.question.equals("debs-q1-plug-5min")){
            builder.setBolt(this.question, new DebsQ1HouseBolt(redisHost, 5l), partitions).customGrouping("netty", new PartitionGrouping());
        }  else if(this.question.equals("debs-q1-plug-15min")){
            builder.setBolt(this.question, new DebsQ1HouseBolt(redisHost, 15l), partitions).customGrouping("netty", new PartitionGrouping());
        }  else if(this.question.equals("debs-q1-plug-60min")){
            builder.setBolt(this.question, new DebsQ1HouseBolt(redisHost, 60l), partitions).customGrouping("netty", new PartitionGrouping());
        }  else if(this.question.equals("debs-q1-plug-120min")){
            builder.setBolt(this.question, new DebsQ1HouseBolt(redisHost, 120l), partitions).customGrouping("netty", new PartitionGrouping());
        } else if(this.question.equals("debs-q3-house-15min")) {
            builder.setBolt(this.question, new DebsQ3HouseBolt(redisHost, 15l), partitions).customGrouping("netty", new PartitionGrouping());
        } else{
            System.out.println("Could not find any routine for the given question `" + question + "`");
            System.exit(1);
        }

        builder.setBolt("result", new ResultBolt(mongoDbHost, mongoDbPort, mongoDbName), partitions).fieldsGrouping(this.question, new Fields("partition"));

        Config conf = new Config();
        conf.setNumWorkers(80);
        conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 30000);
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 1);

        StormSubmitter.submitTopology(this.question + "-topology", conf, builder.createTopology());

        System.out.println("Waiting 5s before shutdown...");
        try {
            Thread.sleep(5000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }

        System.exit(0);

    }

    private ArrayList<HostAndPort> parseHosts(String hostList){
        ArrayList<HostAndPort> hosts = new ArrayList<>();
        String[] hostsStr = hostList.split(";");
        for(String host : hostsStr){
            hosts.add(HostAndPort.fromString(host));
        }

        return hosts;
    }


    public static void main(String[] args) {
        TopologyHelper topologyHelper = new TopologyHelper();
        JCommander j = new JCommander(topologyHelper, args);

        if(topologyHelper.question == null || topologyHelper.dataset == null){
            j.usage();
            System.exit(1);
        }

        try {
            topologyHelper.start();
        } catch (URISyntaxException e){
            System.out.print("Redis uri does not comply with the URI Syntax of Java.");
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e){
            System.out.println("Could not find properties file `speed.properties` in jar file");
            e.printStackTrace();
            System.exit(1);
        } catch (AlreadyAliveException e){
            System.out.println("Topology is already running - failed!");
            e.printStackTrace();
            System.exit(1);
        } catch (InvalidTopologyException e){
            e.printStackTrace();
            System.exit(1);
        }
    }






}