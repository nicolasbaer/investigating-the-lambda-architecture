package ch.uzh.ddis.thesis.lambda_architecture.batch.time_window;

import ch.uzh.ddis.thesis.lambda_architecture.batch.cache.Timestamped;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public interface TimeWindow<E extends Timestamped> {


    public boolean isInWindow(E event);


    public void addEvent(E event);


    public long getWindowStart();


    public long getWindowEnd();



}
