package ch.uzh.ddis.thesis.lambda_architecture.batch.serde;

import org.apache.commons.lang.SerializationUtils;
import org.apache.samza.serializers.Serde;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class GenericSerde implements Serde<GenericData> {

    @Override
    public GenericData fromBytes(byte[] bytes) {
        if (bytes == null){
            return null;
        }

        return (GenericData) SerializationUtils.deserialize(bytes);
    }

    @Override
    public byte[] toBytes(GenericData o) {
        return SerializationUtils.serialize(o);
    }
}
