package ch.uzh.ddis.thesis.lambda_architecture.batch.cache;

import ch.uzh.ddis.thesis.lambda_architecture.data.Timestamped;

import java.io.Serializable;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
final class DateItem implements Timestamped, Serializable{

    private final long timestamp;

    public DateItem(long timestamp){
        this.timestamp = timestamp;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }
}
