package ch.uzh.ddis.thesis.lambda_architecture.kafka.serializer;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.commons.lang.SerializationUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class ListSerializer implements Encoder<List<Object>>, Decoder<List<Object>> {

    /**
     * Kafka will call the serializer with one argument, but currently there's no use for this.
     * Therefore the constructor is empty.
     * @param properties
     */
    public ListSerializer(VerifiableProperties properties){

    }

    @Override
    public List<Object> fromBytes(byte[] bytes) {
        return (List<Object>) SerializationUtils.deserialize(bytes);
    }

    @Override
    public byte[] toBytes(List<Object> objects) {
        return SerializationUtils.serialize((ArrayList<Object>)objects);
    }
}
