package ch.uzh.ddis.thesis.lambda_architecture.data.timewindow;

import ch.uzh.ddis.thesis.lambda_architecture.data.SimpleTimestamp;
import org.junit.Assert;
import org.junit.Test;

public class TumblingWindowTest {

    @Test
    public void testAddEvent() throws Exception {
        // jump one window
        TimeWindow<SimpleTimestamp> window = new TumblingWindow<>(60);
        window.addEvent(new SimpleTimestamp(10));
        window.addEvent(new SimpleTimestamp(40));
        window.addEvent(new SimpleTimestamp(80));

        Assert.assertEquals(70, window.getWindowStart());
        Assert.assertEquals(129, window.getWindowEnd());


        // jump two windows
        window = new TumblingWindow<>(60);
        window.addEvent(new SimpleTimestamp(10));
        window.addEvent(new SimpleTimestamp(40));
        window.addEvent(new SimpleTimestamp(170));

        Assert.assertEquals(130, window.getWindowStart());
        Assert.assertEquals(189, window.getWindowEnd());


        // jump backwards
        window = new TumblingWindow<>(60);
        window.addEvent(new SimpleTimestamp(100));
        window.addEvent(new SimpleTimestamp(40));

        Assert.assertEquals(100, window.getWindowStart());
        Assert.assertEquals(159, window.getWindowEnd());


        // jump none
        window = new TumblingWindow<>(60);
        window.addEvent(new SimpleTimestamp(100));
        window.addEvent(new SimpleTimestamp(100));

        Assert.assertEquals(100, window.getWindowStart());
        Assert.assertEquals(159, window.getWindowEnd());

        // jump none
        window = new TumblingWindow<>(60);
        window.addEvent(new SimpleTimestamp(100));
        window.addEvent(new SimpleTimestamp(159));

        Assert.assertEquals(100, window.getWindowStart());
        Assert.assertEquals(159, window.getWindowEnd());

        // jump one
        window = new TumblingWindow<>(60);
        window.addEvent(new SimpleTimestamp(100));
        window.addEvent(new SimpleTimestamp(160));

        Assert.assertEquals(160, window.getWindowStart());
        Assert.assertEquals(219, window.getWindowEnd());
    }
}