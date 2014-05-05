package ch.uzh.ddis.thesis.lambda_architecture.batch.serde;

import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class LongSerdeFactory implements SerdeFactory<Long>{

    @Override
    public Serde<Long> getSerde(String s, Config config) {
        return new LongSerde();
    }
}
