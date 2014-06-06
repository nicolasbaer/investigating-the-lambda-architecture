package ch.uzh.ddis.thesis.lambda_architecture.data.debs;

import org.junit.Assert;
import org.junit.Test;

public class DebsDataEntryTest {

    @Test
    public void testInit(){
        String entryStr = "2967740693,1379879533,82.042,0,1,0,12";

        DebsDataEntry debs = new DebsDataEntry(entryStr);

        Assert.assertEquals(debs.getRowId(), 2967740693l);
        Assert.assertEquals(debs.getTimestamp(), 1379879533000l);
        Assert.assertEquals(debs.getValue(), 82.042d, 0);
        Assert.assertEquals(debs.getType(), DebsDataTypes.Measurement.Work);
        Assert.assertEquals(debs.getPlugId(), 1);
        Assert.assertEquals(debs.getHouseholdId(), 0);
        Assert.assertEquals(debs.getHouseId(), 12);
    }

    @Test
    public void testGetId() throws Exception {
        String entryStr = "2967740693,1379879533,82.042,0,1,0,12";

        DebsDataEntry debs = new DebsDataEntry(entryStr);
        Assert.assertEquals(debs.getId(), "120001");
    }

    @Test
    public void testGetPartitionKey() throws Exception {
        String entryStr = "2967740693,1379879533,82.042,0,1,0,12";

        DebsDataEntry debs = new DebsDataEntry(entryStr);
        Assert.assertEquals(debs.getPartitionKey(), "12");
    }

    @Test
    public void testGetTimestamp() throws Exception {
        String entryStr = "2967740693,1379879533,82.042,0,1,0,12";

        DebsDataEntry debs = new DebsDataEntry(entryStr);
        Assert.assertEquals(debs.getTimestamp(), 1379879533000l);
    }

    @Test
    public void testGetTopic() throws Exception {
        String entryStr = "2967740693,1379879533,82.042,0,1,0,12";

        DebsDataEntry debs = new DebsDataEntry(entryStr);
        Assert.assertEquals(debs.getTopic(), DebsDataTypes.Measurement.Work.name());
    }
}