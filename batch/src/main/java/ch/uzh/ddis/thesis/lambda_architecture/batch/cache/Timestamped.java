package ch.uzh.ddis.thesis.lambda_architecture.batch.cache;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public interface Timestamped {

    /**
     * @return timestamp in ms
     */
    public long getTimestamp();

}
