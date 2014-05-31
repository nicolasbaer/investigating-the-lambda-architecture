package ch.uzh.ddis.thesis.lambda_architecture.coordination;

import ch.uzh.ddis.thesis.lambda_architecture.coordination.producer.CSVAdaptor;
import ch.uzh.ddis.thesis.lambda_architecture.coordination.producer.KafkaProducer;
import ch.uzh.ddis.thesis.lambda_architecture.coordination.producer.SystemTimeSynchronizer;
import ch.uzh.ddis.thesis.lambda_architecture.data.SRBench.SRBenchDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.data.SRBench.SRBenchDataFactory;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class Coordinator {

    @Parameter(names = "-kafka-properties", description = "path to kafka properties", required = true)
    public String kafkaPropertiesPath;

    @Parameter(names = "-dataset", description = "srbench or debs", required = true)
    public String dataset = null;

    @Parameter(names = "-path", description = "path to process files from. all files within the given path and the ending `csv` are considered.", required = true)
    public String path = null;

    @Parameter(names = "-topic", description = "Kafka topic prefix")
    public String topic = "";

    @Parameter(names = "-startSysTime", description = "system time to start producing in ms")
    public long startSysTime = System.currentTimeMillis() + 5000;

    @Parameter(names = "-startDataTime", description = "start time of the first data item.")
    public long startDataTime = -1;

    @Parameter(names = "-ticksPerMs", description = "data ticks (ms) per system ms")
    public long ticksPerMs = 1000;

    @Parameter(names = "-file-ending", description = "file ending to look for in path, default=csv")
    public String fileEnding = "csv";

    @Parameter(names = "-dataTopic", description = "generates the topic based on the data e.g. for srbench it will generate a topic for each observation")
    public boolean dataTopic = false;

    /**
     * Starts a CSVAdaptor for each csv file in the path and pipes the data through a system time synchronizer
     * to Kafka.
     *
     * @throws FileNotFoundException The path did not contain any csv files
     * @throws InterruptedException
     */
    public void start() throws InterruptedException, IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(this.kafkaPropertiesPath));

        KafkaProducer<SRBenchDataEntry> producer = new KafkaProducer<>(properties, this.topic, this.dataTopic);
        SystemTimeSynchronizer<SRBenchDataEntry> synchronizer = new SystemTimeSynchronizer(producer, this.startSysTime, this.ticksPerMs, this.startDataTime);

        Collection<File> files = this.getFilesFromPath();
        ExecutorService executor = Executors.newFixedThreadPool(files.size() + 1);

        if(files.isEmpty()){
            throw new FileNotFoundException("No files found in the specified directory.");
        }

        for(File file : files){
            int queueId = synchronizer.registerDataInput();
            CSVAdaptor<SRBenchDataEntry> csvAdaptor = new CSVAdaptor<>(file, synchronizer, new SRBenchDataFactory(), queueId);
            executor.execute(csvAdaptor);
        }

        executor.execute(synchronizer);

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }


    /**
     * Finds all `.csv` files in the specified path.
     *
     * @return list of csv files in the path
     */
    public Collection<File> getFilesFromPath(){
        final ArrayList<File> files = new ArrayList<>();
        return FileUtils.listFiles(new File(this.path), new String[]{this.fileEnding}, false);
    }


    public static void main(String[] args) {
        Coordinator coordinator = new Coordinator();
        JCommander j = new JCommander(coordinator, args);

        if(coordinator.kafkaPropertiesPath == null || coordinator.path == null || coordinator.dataset == null){
            j.usage();
            System.exit(1);
        }

        try {
            coordinator.start();
        } catch (IOException | InterruptedException e){
            System.out.println("Something went terribly wrong! error = `" + e + "`");
        }
    }
}
