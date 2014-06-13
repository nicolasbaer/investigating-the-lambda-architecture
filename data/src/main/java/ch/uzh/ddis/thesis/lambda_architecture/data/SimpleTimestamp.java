package ch.uzh.ddis.thesis.lambda_architecture.data;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class SimpleTimestamp implements Timestamped{

    private final long timestamp;

    public SimpleTimestamp(final long timestamp){
        this.timestamp = timestamp;
    }


    @Override
    public long getTimestamp() {
        return this.timestamp;
    }
}
