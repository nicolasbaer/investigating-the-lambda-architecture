package ch.uzh.ddis.thesis.lambda_architecture.data.serde;

import ch.uzh.ddis.thesis.lambda_architecture.data.debs.DebsDataEntry;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class DebsSerdeFactory implements SerdeFactory<DebsDataEntry>{

    @Override
    public Serde<DebsDataEntry> getSerde(String s, Config config) {
        return new DebsSerde();
    }
}
