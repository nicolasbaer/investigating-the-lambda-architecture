package ch.uzh.ddis.thesis.lambda_architecture.data;

/**
 * Holds an offset and a timestamp in order to keep track of offsets within time windows.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class TimestampedOffset implements Timestamped{
    private final long timestamp;
    private final String offset;

    public TimestampedOffset(Timestamped event, String offset){
        this.timestamp = event.getTimestamp();
        this.offset = offset;
    }

    @Override
    public long getTimestamp() {
        return this.timestamp;
    }

    public String getOffset() {
        return this.offset;
    }
}
