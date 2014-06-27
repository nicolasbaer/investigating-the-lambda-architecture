package ch.uzh.ddis.thesis.lambda_architecture.data.serde;

import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class DoubleSerdeFactory implements SerdeFactory<Double>{

    @Override
    public Serde<Double> getSerde(String s, Config config) {
        return new DoubleSerde();
    }
}
