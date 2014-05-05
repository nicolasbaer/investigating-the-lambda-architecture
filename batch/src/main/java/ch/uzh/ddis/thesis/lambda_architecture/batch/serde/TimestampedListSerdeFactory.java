package ch.uzh.ddis.thesis.lambda_architecture.batch.serde;

import ch.uzh.ddis.thesis.lambda_architecture.batch.cache.Timestamped;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;

import java.util.ArrayList;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class TimestampedListSerdeFactory implements SerdeFactory<ArrayList<Timestamped>>{

    @Override
    public Serde<ArrayList<Timestamped>> getSerde(String s, Config config) {
        return new TimestampedListSerde();
    }
}
