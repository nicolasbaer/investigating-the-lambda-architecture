package ch.uzh.ddis.thesis.lambda_architecture.data.serde;

import ch.uzh.ddis.thesis.lambda_architecture.data.SRBench.SRBenchDataEntry;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.commons.lang.SerializationUtils;
import org.apache.samza.serializers.Serde;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class SRBenchSerde implements Serde<SRBenchDataEntry>, Encoder<SRBenchDataEntry> {

    public SRBenchSerde(){

    }

    /**
     * Kafka needs a constructor with the properties parameter, although we do not use the properties here. If this
     * is not present, the Kafka API will throw an Exception.
     * @param properties kafka properties
     */
    public SRBenchSerde(VerifiableProperties properties){
        this();
    }

    @Override
    public SRBenchDataEntry fromBytes(byte[] bytes) {
        if (bytes == null){
            return null;
        }

        return (SRBenchDataEntry) SerializationUtils.deserialize(bytes);
    }

    @Override
    public byte[] toBytes(SRBenchDataEntry o) {
        return SerializationUtils.serialize(o);
    }
}
