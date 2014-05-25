package ch.uzh.ddis.thesis.lambda_architecture.data.serde;

import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;

import java.util.LinkedList;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class LinkedListSerdeFactory implements SerdeFactory<LinkedList>{

    @Override
    public Serde<LinkedList> getSerde(String s, Config config) {
        return new LinkedListSerde();
    }
}
