package ch.uzh.ddis.thesis.lambda_architecture.batch.cache;

import ch.uzh.ddis.thesis.lambda_architecture.batch.serde.GenericData;
import com.google.common.base.Optional;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Caches a sliding window using the provided storage.
 * The window is defined with the following range [start, end)
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class SlidingWindowCache<E extends Timestamped> implements TimeWindowCache<E>{
    private final String keyWindowStart = "window-start";
    private final String keyWindowEnd = "window-end";
    private final KeyValueStore<String, GenericData> store;
    private final long sizeMs;
    private final long rangeMs;

    private long currentWindowStart = 0;
    private long currentWindowEnd = 0;

    /**
     * Initializes a sliding window with the given size and range.
     * For example a sliding window of 10ms every 2ms would mean: sizeMs=10, rangeMs=2
     * @param store key value store used for persistence
     * @param sizeMs size of the tumbling widow in milliseconds
     * @param rangeMs range of time window
     */
    public SlidingWindowCache(KeyValueStore<String, GenericData> store, long sizeMs, long rangeMs){
        this.store = store;
        this.sizeMs = sizeMs;
        this.rangeMs = rangeMs;

        Optional<GenericData> possibleWindowStart = Optional.fromNullable(store.get(keyWindowStart));
        if(possibleWindowStart.isPresent()){
            this.currentWindowStart = (long) possibleWindowStart.get().getData();
        }

        Optional<GenericData> possibleWindowEnd = Optional.fromNullable(store.get(keyWindowEnd));
        if(possibleWindowEnd.isPresent()){
            this.currentWindowEnd = (long) possibleWindowEnd.get().getData();
        }
    }

    @Override
    public void cache(E message) {
        if(currentWindowStart == 0){
            resetWindow(message.getTimestamp());
            this.putMessage(message);

            return;
        }

        if(message.getTimestamp() > currentWindowEnd){
            // find out how many ranges to jump
            int diff = (int) Math.ceil((message.getTimestamp() - currentWindowEnd) / new Double(rangeMs));
            long newEnd = currentWindowEnd + (diff * rangeMs) + 1;
            resetWindow(newEnd - sizeMs);
        }

        this.putMessage(message);
    }

    @Override
    public Iterator<Entry<String, GenericData>> retrieve() {
        String windowStartIndex = this.getKeyZeroPrefixed(currentWindowStart);
        return this.store.range(windowStartIndex, String.valueOf(currentWindowEnd));
    }

    private void resetWindow(long start){
        String windowStartIndex = this.getKeyZeroPrefixed(this.currentWindowStart);
        String rangeEndIndex = this.getKeyZeroPrefixed(this.currentWindowStart + (rangeMs - 1));
        KeyValueIterator<String, GenericData> it = this.store.range(windowStartIndex, rangeEndIndex);
        while(it.hasNext()){
            String key = it.next().getKey();
            this.store.delete(key);
        }

        this.currentWindowStart = start;
        this.currentWindowEnd = start + (sizeMs -1);

        this.store.put(keyWindowStart, new GenericData(currentWindowStart));
        this.store.put(keyWindowEnd, new GenericData(currentWindowEnd));
    }

    /**
     * Since we store the key as string to optimize time for range queries,
     * we have to store the strings, so that a range query returns the right values with
     * respect to the limitation of leveldb to use lexicographical ordering.
     *
     * @param message message to store
     */
    private void putMessage(final E message){
        // the key has to be prefixed with zeros in order to query it lexicographically
        String key = this.getKeyZeroPrefixed(message.getTimestamp());

        Optional<GenericData> optionalEntry = Optional.fromNullable(this.store.get(key));
        if(optionalEntry.isPresent()){
            ArrayList<E> messages = (ArrayList<E>) optionalEntry.get().getData();
            messages.add(message);
            // it's safe to assume the list is serializable, since we use an array list
            store.put(key, new GenericData<ArrayList>(messages));
        } else{
            ArrayList<E> messages = new ArrayList<E>();
            messages.add(message);
            this.store.put(key, new GenericData<ArrayList>(messages));
        }
    }

    private String getKeyZeroPrefixed(long value){
        String key;
        int length = String.valueOf(this.currentWindowEnd).length();
        if(length > String.valueOf(this.currentWindowStart).length()){
            StringBuilder stringBuilder = new StringBuilder();
            String format = stringBuilder.append("%0").append(length).append("d").toString();
            key = String.format(format, value);
        } else{
            key = String.valueOf(value);
        }

        return key;
    }

    @Override
    public long getStartTime() {
        return this.currentWindowStart;
    }

    @Override
    public long getEndTime() {
        return this.currentWindowEnd;
    }
}
