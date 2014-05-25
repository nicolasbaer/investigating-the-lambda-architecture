package ch.uzh.ddis.thesis.lambda_architecture.batch.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.config.Config;

import javax.naming.ConfigurationException;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class HadoopConfigurationFactory {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Creates a hadoop configuration from the samza configuration parameters.
     * It will check the samza config for the key `hadoop.config.path` and add all configuration files
     * to the hadoop config. The produced configuration is compliant with pig.
     *
     * @param config samza configuration
     * @return hadoop configuration
     * @throws ConfigurationException parameter `hadoop.config.path` was not found
     * @throws IOException problem opening the hadoop configuration files.
     */
    public static Configuration makeConfiguration(Config config) throws ConfigurationException, IOException {
        Configuration configuration = new Configuration();

        if(!config.containsKey("hadoop.config.path")){
            throw new ConfigurationException("missing `hadoop.config.path` in configuration file");
        }

        String hadoopConfigStr = config.get("hadoop.config.path");
        File hadoopConfDir = new File(hadoopConfigStr);
        if(hadoopConfDir.exists() && hadoopConfDir.isDirectory() && hadoopConfDir.canRead()){
            for(File file : hadoopConfDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(".xml");
                }
            })){
                configuration.addResource(new Path(file.getPath()));
            }

            // unless this is specified pig will ignore the configuration files added here...
            configuration.set("pig.use.overriden.hadoop.configs", "true");

        } else{
            throw new IOException("could not read hadoop config directory `"+ hadoopConfigStr +"`");
        }



        return configuration;
    }
}
