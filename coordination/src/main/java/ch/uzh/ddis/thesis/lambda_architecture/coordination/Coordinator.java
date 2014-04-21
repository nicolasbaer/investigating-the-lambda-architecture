package ch.uzh.ddis.thesis.lambda_architecture.coordination;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class Coordinator {

    @Parameter(names = "-path", description = "path to process files, use regex to specify multiple files.", required = true)
    public String path = null;

    @Parameter(names = "-topic", description = "Kafka topic to produce to", required = true)
    public String topic = null;

    @Parameter(names = "-partitions", description = "number of partitions for the given topic")
    public Integer partitions = 1;

    @Parameter(names = "-topic_index", description = "index of value in csv to append to the given kafka topic")
    public Integer topicIndex = null;

    @Parameter(names = "-yarn", description = "run on apache yarn")
    public boolean yarn = false;


    public void run(){
        if(yarn){
            this.runYarn();
        } else{
            this.runLocal();
        }
    }

    public void runLocal(){

    }


    public void runYarn(){

    }


    public static void main(String[] args) {
        Coordinator coordinator = new Coordinator();
        JCommander j = new JCommander(coordinator);

        if(coordinator.topic == null || coordinator.path == null){
            j.usage();
            System.exit(1);
        }

        coordinator.run();
    }
}
