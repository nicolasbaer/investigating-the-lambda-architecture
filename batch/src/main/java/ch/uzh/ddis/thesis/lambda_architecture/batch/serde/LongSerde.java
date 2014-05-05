package ch.uzh.ddis.thesis.lambda_architecture.batch.serde;

import com.google.common.primitives.Longs;
import org.apache.samza.serializers.Serde;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class LongSerde implements Serde<Long> {

    @Override
    public Long fromBytes(byte[] bytes) {
        return Longs.fromByteArray(bytes);
    }

    @Override
    public byte[] toBytes(Long o) {
        return Longs.toByteArray(o);
    }
}
