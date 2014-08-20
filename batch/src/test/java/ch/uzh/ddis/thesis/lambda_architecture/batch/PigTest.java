package ch.uzh.ddis.thesis.lambda_architecture.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class PigTest {



    @Ignore
    @Test
    public void testHDFSWrite(){

        Configuration conf = new Configuration();
        conf.addResource(new Path("/Users/nicolas/thesis/code/deploy/yarn/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/Users/nicolas/thesis/code/deploy/yarn/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/Users/nicolas/thesis/code/deploy/yarn/etc/hadoop/mapred-site.xml"));

        Iterator<Map.Entry<String, String>> iter = conf.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }

        conf.set("pig.use.overriden.hadoop.configs", "true");


        try {
            FileSystem hdfs = FileSystem.get(conf);

            hdfs.delete(new Path("/test"), false);
            FSDataOutputStream outputStream = hdfs.create(new Path("/test2"));
            BufferedWriter br = new BufferedWriter( new OutputStreamWriter( outputStream, StandardCharsets.UTF_8) );


            for(int i = 0; i < 2000; i++) {
                br.write("" + i + "\n");
            }

            br.close();
            outputStream.close();


            PigContext context = new PigContext(ExecType.MAPREDUCE, conf);
            PigServer pigServer = new PigServer(context);


            Map<String, String> params = new HashMap<>();
            params.put("out_path", "/my-pig-with-param3");
            pigServer.registerScript(this.getClass().getResourceAsStream("/sample.pig"), params);



        } catch (IOException e) {
            e.printStackTrace();
        }


    }


}
