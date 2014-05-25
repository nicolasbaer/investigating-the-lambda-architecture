package ch.uzh.ddis.thesis.lambda_architecture.batch.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.samza.config.Config;

import javax.naming.ConfigurationException;
import java.io.IOException;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class HDFSFactory {

    public static FileSystem makeHDFSFileSystem(Config config) throws IOException, ConfigurationException {
        Configuration conf = HadoopConfigurationFactory.makeConfiguration(config);
        return FileSystem.get(conf);
    }

}
