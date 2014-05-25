package ch.uzh.ddis.thesis.lambda_architecture.data.serde;

import ch.uzh.ddis.thesis.lambda_architecture.data.Timestamped;
import org.apache.commons.lang.SerializationUtils;
import org.apache.samza.serializers.Serde;

import java.util.ArrayList;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class TimestampedListSerde implements Serde<ArrayList<Timestamped>> {

    @Override
    public ArrayList<Timestamped> fromBytes(byte[] bytes) {
        return (ArrayList<Timestamped>) SerializationUtils.deserialize(bytes);
    }

    @Override
    public byte[] toBytes(ArrayList<Timestamped> o) {
        return SerializationUtils.serialize(o);
    }
}
