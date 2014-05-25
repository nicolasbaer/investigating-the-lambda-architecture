package ch.uzh.ddis.thesis.lambda_architecture.data.serde;

import org.apache.commons.lang.SerializationUtils;
import org.apache.samza.serializers.Serde;

import java.util.LinkedList;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class LinkedListSerde implements Serde<LinkedList> {

    @Override
    public LinkedList fromBytes(byte[] bytes) {
        return (LinkedList) SerializationUtils.deserialize(bytes);
    }

    @Override
    public byte[] toBytes(LinkedList o) {
        return SerializationUtils.serialize(o);
    }
}
