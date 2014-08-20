package ch.uzh.ddis.thesis.lambda_architecture.batch.cache;

import ch.uzh.ddis.thesis.lambda_architecture.data.serde.GenericData;
import com.google.common.base.Optional;
import org.apache.samza.storage.kv.KeyValueStore;

/**
 * This class is depricated as it was used to provide an alternative checkpoint solution for Samza.
 *
 * Offset cache to check whether an offset was already consumed.
 * According to the Kafka API the offset will grow over time,
 * a simple check involves storing the last consumed offset into
 * a persistent storage and check it against the current offset.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
@Deprecated
public class OffsetCache {
    private final static String lastOffsetId = "lastOffset";
    private final KeyValueStore<String, GenericData> store;

    public OffsetCache(KeyValueStore<String, GenericData> store){
        this.store = store;

    }


    /**
     * Checks if the current offset was already processed and caches the given offset.
     * @param currentOffset
     * @return true if processed, false otherwise
     */
    public boolean checkCache(final long currentOffset){
        boolean processed = true;
        Optional<GenericData> possibleLastOffset = Optional.fromNullable(this.store.get(lastOffsetId));
        if(possibleLastOffset.isPresent()){
            long lastOffset = (Long) possibleLastOffset.get().getData();
            if(currentOffset > lastOffset){
                processed = false;
            }
        }

        this.store.put(lastOffsetId, new GenericData(currentOffset));

        return processed;
    }

    /**
     * Caches the current offset.
     * @param currentOffset offset to cache
     */
    public void cache(final long currentOffset){
        this.store.put(lastOffsetId, new GenericData(currentOffset));
    }

}
