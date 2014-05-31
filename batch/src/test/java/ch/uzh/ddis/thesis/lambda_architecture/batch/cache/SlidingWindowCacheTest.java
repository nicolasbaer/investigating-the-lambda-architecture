package ch.uzh.ddis.thesis.lambda_architecture.batch.cache;

import ch.uzh.ddis.thesis.lambda_architecture.data.serde.GenericData;
import ch.uzh.ddis.thesis.lambda_architecture.data.serde.GenericSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class SlidingWindowCacheTest {

    @Before
    public void setup(){

    }

    @After
    public void tearDown(){

    }

    @Test
    public void testCache() throws Exception {
        KeyValueStore<String, GenericData> store = null;
        try {
            store = new HashKV<>(new StringSerde(StandardCharsets.UTF_8.toString()), new GenericSerde());
        } catch (IOException e){
            Assert.assertTrue(false);
        }
        SlidingWindowCache cache = new SlidingWindowCache<DateItem>(store, 5l, 2l);

        for (long i = 1; i < 7; i++) {
            cache.cache(new DateItem(i));
        }


        for (long i = 17; i < 38; i++){
            cache.cache(new DateItem(i));
        }

        Iterator<Entry<String, GenericData>> it = cache.retrieve();
        List<DateItem> result = new ArrayList<>();
        while(it.hasNext()){
            result.addAll((ArrayList<DateItem>) it.next().getValue().getData());
        }

        Assert.assertEquals(result.size(), 5);
        Assert.assertEquals(result.get(result.size()-1).getTimestamp(), 37l);

    }


    // TODO: check for compatibility with esper, time windows should match!


}
