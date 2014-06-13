package ch.uzh.ddis.thesis.lambda_architecture.utils.kafkaconsumer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import kafka.admin.AdminUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class KafkaTopicManager {

    @Parameter(names = "-topic", description = "topic name", required = true)
    public String topic;

    @Parameter(names = "-partitions", description = "number of partitions")
    public int partitions = 1;

    @Parameter(names = "-replication", description = "replication factor")
    public int replication = 1;


    /***
     * Creates a Kafka topic with the given properties.
     * @throws IOException in case the properties file is not in the resources directory.
     */
    public void createTopic() throws IOException{
        Properties properties = new Properties();
        properties.load(new FileInputStream("kafka.properties"));

        String zookeeperUrl = properties.getProperty("kafka.zookeeper.url");

        int sessionTimeoutMs = 10000;
        int connectionTimeoutMs = 10000;
        ZkClient zkClient = new ZkClient(zookeeperUrl, 10000, 10000, new ZkSerializer() {
            @Override
            public byte[] serialize(Object data) throws ZkMarshallingError {
                return String.valueOf(data).getBytes();
            }

            @Override
            public Object deserialize(byte[] bytes) throws ZkMarshallingError {
                if(bytes == null){
                    return null;
                } else{
                    return new String(bytes, StandardCharsets.UTF_8);
                }
            }
        });

        Properties topicConfig = new Properties();
        AdminUtils.createTopic(zkClient, this.topic, this.partitions, this.replication, topicConfig);
    }


    public static void main(String[] args) {
        KafkaTopicManager manager = new KafkaTopicManager();
        try {
            JCommander j = new JCommander(manager, args);
            if(manager.topic == null){
                j.usage();
                System.exit(1);
            }

            try {
                manager.createTopic();
            } catch (IOException e){
                e.printStackTrace();
                System.exit(1);
            }

        } catch (ParameterException e){
            System.out.println("please specify a topic with -topic <name>");
            System.exit(1);
        }




    }


}
