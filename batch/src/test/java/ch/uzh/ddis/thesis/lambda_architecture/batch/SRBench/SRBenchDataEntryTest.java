package ch.uzh.ddis.thesis.lambda_architecture.batch.SRBench;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class SRBenchDataEntryTest {

    private final String csvEntry = "1253570400,HPROS,WindGust,3.0,milesPerHour,WindObservation";

    @Test
    public void testGetId() throws Exception {

        SRBenchDataEntry entry = new SRBenchDataEntry(csvEntry);

        Assert.assertEquals(entry.getId(), "1253570400HPROSWindObservationWindGust");
    }
}
