package ch.uzh.ddis.thesis.lambda_architecture.batch.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.samza.config.MapConfig;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URL;
import java.util.HashMap;

public class HadoopConfigurationFactoryTest {

    @Ignore
    @Test
    public void testMakeConfiguration() throws Exception{

        URL hadoopConfigUrl = this.getClass().getResource("/hadoop-configs");

        HashMap map = new HashMap<String, String>();
        map.put("hadoop.config.path", hadoopConfigUrl.getPath());

        MapConfig samzaConfig = new MapConfig(map);


        Configuration conf = HadoopConfigurationFactory.makeConfiguration(samzaConfig);

        Assert.assertEquals(conf.get("fs.defaultFS"), "hdfs://localhost:8020");
        Assert.assertEquals(conf.get("pig.use.overriden.hadoop.configs"), "true");
    }
}