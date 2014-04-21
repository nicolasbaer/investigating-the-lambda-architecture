package ch.uzh.ddis.thesis.lambda_architecture.batch.cache;

import java.util.List;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 *
 * General Interface for all implementations of time window caches.
 */
public interface TimeWindowCache<E extends Timestamped> {

    /**
     * Caches the given message. This step will also decide to flush messages from the cache in order
     * to ensure the time window is on track.
     * @param message message to cache
     * @param timestamp timestamp of the message to cache
     */
    public void cache(E message);

    /**
     * Retrieve all cached messages.
     *
     * @return list of cached messages within the current window
     */
    public List<E> retrieve();

}
