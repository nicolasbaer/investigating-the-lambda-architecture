package ch.uzh.ddis.thesis.lambda_architecture.data;

import java.io.Serializable;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public interface Timestamped extends Serializable {

    /**
     * @return timestamp in ms
     */
    public long getTimestamp();

}
