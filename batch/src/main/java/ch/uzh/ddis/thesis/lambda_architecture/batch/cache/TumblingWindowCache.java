package ch.uzh.ddis.thesis.lambda_architecture.batch.cache;

import ch.uzh.ddis.thesis.lambda_architecture.batch.serde.GenericData;
import com.google.common.base.Optional;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.LinkedList;
import java.util.List;

/**
 * Caches a tumbling window using the provided storage.
 * The window is defined with the following range [start, end)
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class TumblingWindowCache<E extends Timestamped> implements TimeWindowCache<E>{
    private final String key = "tumbling-window";
    private final String keyWindowStart = "window-start";
    private final String keyWindowEnd = "window-end";
    private final KeyValueStore<String, GenericData> store;
    private final long sizeMs;

    private long currentWindowStart = 0;
    private long currentWindowEnd = 0;

    /**
     *
     * @param store key value store used for persistence
     * @param sizeMs size of the tumbling widow in milliseconds
     */
    public TumblingWindowCache(KeyValueStore<String, GenericData> store, long sizeMs){
        this.store = store;
        this.sizeMs = sizeMs;

        Optional<GenericData> possibleList = Optional.fromNullable(store.get(key));
        if(!possibleList.isPresent()){
            store.put(key, new GenericData(new LinkedList<E>()));
        }

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
        LinkedList<E> list = (LinkedList) store.get(key).getData();

        if(currentWindowStart == 0){
            this.resetWindow(message.getTimestamp());
            list.addLast(message);
            store.put(key, new GenericData(list));

            return;
        }

        if (message.getTimestamp() > currentWindowEnd) {
            list.clear();
            resetWindow(message.getTimestamp());
            list.addLast(message);
        } else{
            list.add(message);
        }

        store.put(key, new GenericData(list));
    }

    @Override
    public List<E> retrieve() {
        return (LinkedList) store.get(key).getData();
    }

    private void resetWindow(long start){
        this.currentWindowStart = start;
        this.currentWindowEnd = start + (sizeMs -1);

        this.store.put(keyWindowStart, new GenericData(currentWindowStart));
        this.store.put(keyWindowEnd, new GenericData(currentWindowEnd));
    }

}
