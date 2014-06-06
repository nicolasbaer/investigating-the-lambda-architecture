package ch.uzh.ddis.thesis.lambda_architecture.data.serde;

import ch.uzh.ddis.thesis.lambda_architecture.data.debs.DebsDataEntry;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.commons.lang.SerializationUtils;
import org.apache.samza.serializers.Serde;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class DebsSerde implements Serde<DebsDataEntry>, Encoder<DebsDataEntry> {

    public DebsSerde(){

    }

    /**
     * Kafka needs a constructor with the properties parameter, although we do not use the properties here. If this
     * is not present, the Kafka API will throw an Exception.
     * @param properties kafka properties
     */
    public DebsSerde(VerifiableProperties properties){
        this();
    }

    @Override
    public DebsDataEntry fromBytes(byte[] bytes) {
        if (bytes == null){
            return null;
        }

        return (DebsDataEntry) SerializationUtils.deserialize(bytes);
    }

    @Override
    public byte[] toBytes(DebsDataEntry o) {
        return SerializationUtils.serialize(o);
    }
}
