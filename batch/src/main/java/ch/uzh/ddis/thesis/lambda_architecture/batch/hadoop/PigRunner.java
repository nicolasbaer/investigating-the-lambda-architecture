package ch.uzh.ddis.thesis.lambda_architecture.batch.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.apache.samza.config.Config;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Provides an abstraction to run pig scripts in a very simple way.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class PigRunner {
    private final static Logger logger = LogManager.getLogger();

    private final Configuration hadoopConf;
    private final PigServer pigServer;

    public PigRunner(Config config) throws IOException, ConfigurationException {
        hadoopConf = HadoopConfigurationFactory.makeConfiguration(config);

        PigContext context = new PigContext(ExecType.MAPREDUCE, hadoopConf);
        pigServer = new PigServer(context);
    }

    public void runScript(InputStream script, Map<String, String> parameters) throws IOException {
        pigServer.registerScript(script, parameters);
    }

}
