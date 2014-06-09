package ch.uzh.ddis.thesis.lambda_architecture.data.serde;

import org.apache.commons.lang.SerializationUtils;
import org.apache.samza.serializers.Serde;

import java.util.HashMap;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class MapSerde implements Serde<HashMap<String, Object>> {

    @Override
    public HashMap<String, Object> fromBytes(byte[] bytes) {
        return (HashMap<String, Object>) SerializationUtils.deserialize(bytes);
    }

    @Override
    public byte[] toBytes(HashMap<String, Object> o) {
        return SerializationUtils.serialize(o);
    }
}
