package ch.uzh.ddis.thesis.lambda_architecture.data.serde;

import ch.uzh.ddis.thesis.lambda_architecture.data.SRBench.SRBenchDataEntry;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class SRBenchSerdeFactory implements SerdeFactory<SRBenchDataEntry>{

    @Override
    public Serde<SRBenchDataEntry> getSerde(String s, Config config) {
        return new SRBenchSerde();
    }
}
