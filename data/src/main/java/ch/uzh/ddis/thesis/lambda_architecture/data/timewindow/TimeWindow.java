package ch.uzh.ddis.thesis.lambda_architecture.data.timewindow;

import ch.uzh.ddis.thesis.lambda_architecture.data.Timestamped;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public interface TimeWindow<E extends Timestamped> {


    public boolean isInWindow(E event);


    public void addEvent(E event);


    public long getWindowStart();


    public E getWindowStartEvent();


    public long getWindowEnd();



}
