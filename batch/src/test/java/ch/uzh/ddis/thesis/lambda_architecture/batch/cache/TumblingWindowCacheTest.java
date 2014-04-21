package ch.uzh.ddis.thesis.lambda_architecture.batch.cache;

import ch.uzh.ddis.thesis.lambda_architecture.batch.serde.GenericData;
import ch.uzh.ddis.thesis.lambda_architecture.batch.serde.GenericSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class TumblingWindowCacheTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Before
    public void setup(){

    }

    @After
    public void tearDown(){

    }

    @Test
    public void testCache() throws Exception {
        KeyValueStore<String, GenericData> store = new HashKV<>(new StringSerde(StandardCharsets.UTF_8.toString()), new GenericSerde());
        TumblingWindowCache cache = new TumblingWindowCache(store, 5);

        for (long i = 1; i < 7; i++) {
            cache.cache(new DateItem(i));
        }

        List<DateItem> result = cache.retrieve();
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.get(0).getTimestamp(), 6l);

        for (long i = 17; i < 38; i++){
            cache.cache(new DateItem(i));
        }

        result = cache.retrieve();
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.get(0).getTimestamp(), 37l);

    }

    @Test
    public void testRetrieve() throws Exception {

    }


    // TODO: check for compatibility with esper, time windows should match!
}
