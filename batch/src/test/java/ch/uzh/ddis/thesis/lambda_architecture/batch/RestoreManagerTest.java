package ch.uzh.ddis.thesis.lambda_architecture.batch;

import ch.uzh.ddis.thesis.lambda_architecture.batch.cache.HashKV;
import ch.uzh.ddis.thesis.lambda_architecture.data.serde.GenericData;
import ch.uzh.ddis.thesis.lambda_architecture.data.serde.GenericSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RestoreManagerTest {

    @Test
    public void testCheckRestore() {
        KeyValueStore<String, GenericData> store = null;
        try {
             store = new HashKV<>(new StringSerde(StandardCharsets.UTF_8.toString()), new GenericSerde());
        } catch (IOException e){
            Assert.assertTrue(false);
        }
        RestoreManager restoreManager = new RestoreManager(store);

        Assert.assertFalse(restoreManager.checkRestore());
        Assert.assertFalse(restoreManager.checkRestore());

        store.put("test", new GenericData("test"));
        RestoreManager restoreManager1 = new RestoreManager(store);

        Assert.assertTrue(restoreManager1.checkRestore());
        Assert.assertFalse(restoreManager1.checkRestore());

    }
}