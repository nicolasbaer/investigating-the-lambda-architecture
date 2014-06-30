package ch.uzh.ddis.thesis.lambda_architecture.partitioner;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.data.IDataFactory;
import ch.uzh.ddis.thesis.lambda_architecture.data.SRBench.SRBenchDataFactory;
import ch.uzh.ddis.thesis.lambda_architecture.data.debs.DebsDataFactory;
import ch.uzh.ddis.thesis.lambda_architecture.data.partitioner.HashBucketPartitioner;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.io.FilenameUtils;

import java.io.*;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class FilePartitioner {

    @Parameter(names = "-file", description = "path to file to partition", required = true)
    public String filePath;

    @Parameter(names = "-dataset", description = "dataset `debs` or `srbench`", required = true)
    public String dataset;

    @Parameter(names = "-partitions", description = "number of partitions", required = true)
    public int partitions;

    @Parameter(names = "-num_output", description = "number of output files, usually the same as partitions")
    public int outputNum = 0;

    @Parameter(names = "-out", description = "path for the partitioned files", required = false)
    public String out;


    public void start(IDataFactory factory) throws IOException {
        File file = new File(this.filePath);

        String path = file.getPath();
        String parent;
        if(this.out == null || !new File(this.out).canWrite()) {
            parent = FilenameUtils.getFullPath(path);
        } else{
            parent = this.out;
        }
        String name = FilenameUtils.getBaseName(path);
        String ending = FilenameUtils.getExtension(path);
        int outputFiles = outputNum == 0 ? this.partitions : outputNum;

        if(file.canRead()){
            File[] files = new File[outputFiles];
            BufferedWriter[] writers = new BufferedWriter[outputFiles];

            for(int i = 0; i < outputFiles; i++){
                File f = new File(parent + name + "-part-" + i + "." + ending);
                files[i] = f;
                BufferedWriter writer = new BufferedWriter(new FileWriter(f));
                writers[i] = writer;
            }

            HashBucketPartitioner hashBucketPartitioner = new HashBucketPartitioner();
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while((line = reader.readLine()) != null){
                IDataEntry entry = factory.makeDataEntryFromCSV(line);

                int partition = hashBucketPartitioner.partition(entry.getPartitionKey(), this.partitions);

                writers[partition % outputFiles].write(line);
                writers[partition % outputFiles].newLine();
            }

            for(int i = 0; i < outputFiles; i++){
                writers[i].flush();
                writers[i].close();
            }

        }
    }


    public static void main(String[] args) {

        FilePartitioner partitioner = new FilePartitioner();
        JCommander j = new JCommander(partitioner, args);

        if(partitioner.filePath == null || partitioner.dataset == null || partitioner.partitions == 0){
            j.usage();
            System.exit(1);
        }

        IDataFactory factory = null;
        if(partitioner.dataset.equals("srbench")) {
            factory = new SRBenchDataFactory();
        } else if(partitioner.dataset.equals("debs")){
            factory = new DebsDataFactory();
        } else{
            System.out.println("Don't know how to handle `" + partitioner.dataset + "` please choose on of the following: `srbench`, `debs`");
            System.exit(1);
        }

        try {
            partitioner.start(factory);
        } catch (IOException e){
            e.printStackTrace();
        }

    }




}
