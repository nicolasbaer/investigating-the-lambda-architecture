package ch.uzh.ddis.thesis.lambda_architecture.data.serde;

import ch.uzh.ddis.thesis.lambda_architecture.data.SRBench.SRBenchDataEntry;
import org.apache.commons.lang.SerializationUtils;
import org.apache.samza.serializers.Serde;
import org.junit.Assert;
import org.junit.Test;

public class SRBenchSerdeTest {

    private final String csvEntry = "1253570400,HPROS,WindGust,3.0,milesPerHour,WindObservation";


    @Test
    public void testFromBytes() throws Exception {
        SRBenchDataEntry entry = new SRBenchDataEntry(csvEntry);
        Serde<SRBenchDataEntry> serde = new SRBenchSerde();

        byte[] serialized = SerializationUtils.serialize(entry);

        SRBenchDataEntry serdeEntry = serde.fromBytes(serialized);

        Assert.assertEquals(entry.getId(), serdeEntry.getId());
    }

    @Test
    public void testToBytes() throws Exception {
        SRBenchDataEntry entry = new SRBenchDataEntry(csvEntry);
        Serde<SRBenchDataEntry> serde = new SRBenchSerde();

        byte[] entrySerde = serde.toBytes(entry);
        byte[] entrySerdeExpected = SerializationUtils.serialize(entry);

        for(int i = 0; i < entrySerde.length; i++){
            Assert.assertTrue(entrySerde[i] == entrySerdeExpected[i]);
        }

    }
}