package ch.uzh.ddis.thesis.lambda_architecture.data.serde;

import org.apache.commons.lang.SerializationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class MapSerdeTest {

    @Test
    public void testFromBytes() throws Exception {
        HashMap<String, Object> map = new HashMap<>(3);
        map.put("test1", new Long(1));
        map.put("test2", new String("test"));
        map.put("test3", new Double(1));

        byte[] bytes = SerializationUtils.serialize(map);

        MapSerde mapSerde = new MapSerde();
        HashMap<String, Object> result = mapSerde.fromBytes(bytes);

        Assert.assertEquals(map, result);

    }

    @Test
    public void testToBytes() throws Exception {
        HashMap<String, Object> map = new HashMap<>(3);
        map.put("test1", new Long(1));
        map.put("test2", new String("test"));
        map.put("test3", new Double(1));

        byte[] bytes = SerializationUtils.serialize(map);

        MapSerde mapSerde = new MapSerde();
        byte[] result = mapSerde.toBytes(map);

        for(int i = 0; i < result.length; i++){
            Assert.assertTrue(result[i] == bytes[i]);
        }
    }
}