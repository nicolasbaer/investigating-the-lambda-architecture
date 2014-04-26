package ch.uzh.ddis.thesis.lambda_architecture.batch.cache;

import ch.uzh.ddis.thesis.lambda_architecture.batch.serde.GenericData;
import ch.uzh.ddis.thesis.lambda_architecture.batch.serde.GenericSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class OffsetCacheTest {

    @Test
    public void testCheckCache() {
        KeyValueStore<String, GenericData> store = null;
        try {
            store = new HashKV<>(new StringSerde(StandardCharsets.UTF_8.toString()), new GenericSerde());
        } catch (IOException e){
            Assert.assertTrue(false);
        }
        OffsetCache cache = new OffsetCache(store);


        for(int i = 0; i < 100; i++){
            cache.cache(i);
        }

        Assert.assertTrue(cache.checkCache(10l));
        Assert.assertFalse(cache.checkCache(200l));
    }
}