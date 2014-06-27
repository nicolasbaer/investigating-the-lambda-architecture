package ch.uzh.ddis.thesis.lambda_architecture.batch.SRBench.task;

import ch.uzh.ddis.thesis.lambda_architecture.data.esper.EsperUpdateListener;
import ch.uzh.ddis.thesis.lambda_architecture.data.SRBench.SRBenchDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.batch.hadoop.HDFSFactory;
import ch.uzh.ddis.thesis.lambda_architecture.batch.hadoop.PigRunner;
import ch.uzh.ddis.thesis.lambda_architecture.data.timewindow.TimeWindow;
import ch.uzh.ddis.thesis.lambda_architecture.data.timewindow.TumblingWindow;
import com.espertech.esper.client.EPRuntime;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;

import java.io.IOException;
import java.util.UUID;

/**
 * Stream Task to answer SRBench Question 1 using esper engine:
 * `Get the rainfall observed once in an hour`
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class SRBenchQ3Hadoop implements StreamTask, InitableTask{
    private static final String esperEngineName = "srbench-q3";
    private EPRuntime esper;
    private TimeWindow<SRBenchDataEntry> timeWindow;
    private EsperUpdateListener esperUpdateListener;

    private FileSystem hdfs;
    private PigRunner pigRunner;

    private final SystemStream resultStream = new SystemStream("kafka", "srbench-q3-results");
    private String id;

    private boolean start = true;

    private FSDataOutputStream hdfsOutputStream;


    @Override
    public void init(Config config, TaskContext taskContext) throws Exception {
        long windowSize = 60l * 60l * 1000l;
        this.timeWindow = new TumblingWindow<>(windowSize);

        this.pigRunner = new PigRunner(config);
        this.hdfs = HDFSFactory.makeHDFSFileSystem(config);

        this.id = "part-" + taskContext.getPartition().getPartitionId() + "-" + UUID.randomUUID();

    }

    @Override
    public void process(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws IOException {
        String message = (String) incomingMessageEnvelope.getMessage();
        SRBenchDataEntry entry = new SRBenchDataEntry(message);

        if(!this.timeWindow.isInWindow(entry) && !start){



            taskCoordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
        }

        this.timeWindow.addEvent(entry);

        if(start){
            start = false;
            this.hdfsOutputStream = hdfs.create(this.getCurrentFilename());
        }


    }

    private Path getCurrentFilename(){
        String file = "hdfs:///srbench/" + this.id + "/" + timeWindow.getWindowStart() + "-" + timeWindow.getWindowEnd();

        return new Path(file);
    }

}
