package ch.uzh.ddis.thesis.lambda_architecture.data.timewindow;

import ch.uzh.ddis.thesis.lambda_architecture.data.SimpleTimestamp;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class SlidingWindowTest {

    @Test
    public void testGetWindowStartEvent() throws Exception {
        TimeWindow<SimpleTimestamp> window = new SlidingWindow<>(100, 10);

        final long start = 0;
        ArrayList<Long> firstTimestamps = new ArrayList<>(100);

        long currentFirst = -1;
        for(int i = 0; i <= 500; i+=5){

            long t = start + i;
            window.addEvent(new SimpleTimestamp(t));

            long first = window.getWindowStartEvent().getTimestamp();
            if(first != currentFirst){
                firstTimestamps.add(first);
                currentFirst = first;


            }
        }

        int i = 0;
        for(long t : firstTimestamps){
            Assert.assertEquals(t, i);

            i+=10;
        }


    }
}