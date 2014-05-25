package ch.uzh.ddis.thesis.lambda_architecture.batch.cache;

import ch.uzh.ddis.thesis.lambda_architecture.data.serde.GenericData;
import ch.uzh.ddis.thesis.lambda_architecture.data.Identifiable;
import com.google.common.base.Optional;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import org.apache.samza.storage.kv.KeyValueStore;

import java.nio.charset.StandardCharsets;

/**
 * Simple Cache mechanism to check whether a message was already processed.
 * It uses two strategies to accomplish this check:
 * 1. Holds a bloom filter to check fast if this message was not processed.
 * 2. the bloom filter has a 1% chance of false positives, therefore a second check looks up the message in the
 * key-value store.
 *
 * @param <E> Identifiable message to check
 */
public class MessageDeliveryGuaranteeCache<E extends Identifiable<String>> {
    private final KeyValueStore<String, GenericData> store;
    private final BloomFilter bloomFilter;

    public MessageDeliveryGuaranteeCache(KeyValueStore<String, GenericData> store, int expectedInsertions){
        this.store = store;
        this.bloomFilter = BloomFilter.create(new IdentifiableFunnel(), expectedInsertions);
    }

    /**
     * Checks whether the message was already processed. If the message was not processed yet, it will be included
     * in the cache.
     * @param message message to check
     * @return processed or not
     */
    public boolean checkAndCache(E message){
        boolean processed = false;
        if(bloomFilter.mightContain(message)){
            // there's a 1% probability that the message was already processed, we need 0% :)
            Optional<GenericData> possibleMessage = Optional.fromNullable(store.get(message.getId()));
            if (possibleMessage.isPresent()){
                processed = true;
            }
        }

        if (!processed){
            bloomFilter.put(message);
            store.put(message.getId(), new GenericData(0));
        }

        return processed;
    }

    private class IdentifiableFunnel implements Funnel<Identifiable<String>>{
        @Override
        public void funnel(Identifiable<String> entry, PrimitiveSink into) {
            into.putString(entry.getId(), StandardCharsets.UTF_8);
        }
    }

}
