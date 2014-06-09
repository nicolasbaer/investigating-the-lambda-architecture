package ch.uzh.ddis.thesis.lambda_architecture.data.serde;

import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;

import java.util.HashMap;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class MapSerdeFactory implements SerdeFactory<HashMap<String, Object>>{

    @Override
    public Serde<HashMap<String, Object>> getSerde(String s, Config config) {
        return new MapSerde();
    }
}
