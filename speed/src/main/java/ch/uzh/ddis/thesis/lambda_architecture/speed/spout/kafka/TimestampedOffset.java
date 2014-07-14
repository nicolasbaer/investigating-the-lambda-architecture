package ch.uzh.ddis.thesis.lambda_architecture.speed.spout.kafka;

import ch.uzh.ddis.thesis.lambda_architecture.data.Timestamped;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class TimestampedOffset implements Timestamped {

    private final long offset;
    private final long timestamp;


    public TimestampedOffset(long offset, long timestamp){
        this.offset = offset;
        this.timestamp = timestamp;
    }


    @Override
    public long getTimestamp() {
        return this.timestamp;
    }


    public long getOffset() {
        return offset;
    }
}
