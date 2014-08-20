package ch.uzh.ddis.thesis.lambda_architecture.batch.cache;

import ch.uzh.ddis.thesis.lambda_architecture.data.serde.GenericData;
import ch.uzh.ddis.thesis.lambda_architecture.data.Timestamped;
import org.apache.samza.storage.kv.Entry;

import java.util.Iterator;

/**
 * This class is depricated as it was used to provide an alternative checkpoint solution for Samza.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 *
 * General Interface for all implementations of time window caches.
 */
@Deprecated
public interface TimeWindowCache<E extends Timestamped> {

    /**
     * Caches the given message. This step will also decide to flush messages from the cache in order
     * to ensure the time window is on track.
     * @param message message to cache
     */
    public void cache(E message);

    /**
     * Retrieve all cached messages.
     *
     * @return list of cached messages within the current window
     */
    public Iterator<Entry<String, GenericData>> retrieve();


    /**
     * @return start time of the current window in milliseconds
     */
    public long getStartTime();


    /**
     * @return end time of the current window in milliseconds
     */
    public long getEndTime();

}
