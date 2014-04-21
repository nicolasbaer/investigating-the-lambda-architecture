package ch.uzh.ddis.thesis.lambda_architecture.batch.cache;

import ch.uzh.ddis.thesis.lambda_architecture.batch.serde.GenericData;
import ch.uzh.ddis.thesis.lambda_architecture.batch.serde.GenericSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class MessageDeliveryGuaranteeCacheTest {


    @Test
    public void checkAndCache() throws IOException {
        KeyValueStore<String, GenericData> store = new HashKV<>(new StringSerde(StandardCharsets.UTF_8.toString()), new GenericSerde());
        MessageDeliveryGuaranteeCache<StringItem> mdgc = new MessageDeliveryGuaranteeCache<>(store, 1010);

        for(int i = 0; i < 1000; i++){
            mdgc.checkAndCache(new StringItem(new Integer(i).toString()));
        }


        Assert.assertTrue(mdgc.checkAndCache(new StringItem(new Integer(100).toString())));

        Assert.assertFalse(mdgc.checkAndCache(new StringItem(new Integer(5000).toString())));
        Assert.assertTrue(mdgc.checkAndCache(new StringItem(new Integer(5000).toString())));

    }
}
