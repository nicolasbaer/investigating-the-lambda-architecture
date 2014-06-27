package ch.uzh.ddis.thesis.lambda_architecture.data.serde;

import kafka.serializer.Encoder;
import org.apache.commons.lang.SerializationUtils;
import org.apache.samza.serializers.Serde;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class DoubleSerde implements Serde<Double>, Encoder<Double> {

    @Override
    public Double fromBytes(byte[] bytes) {
        return (Double) SerializationUtils.deserialize(bytes);
    }

    @Override
    public byte[] toBytes(Double o) {
        return SerializationUtils.serialize(o);
    }
}
