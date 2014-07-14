package ch.uzh.ddis.thesis.lambda_architecture.data.timewindow;

import ch.uzh.ddis.thesis.lambda_architecture.data.Timestamped;

/**
 * Keeps track of a tumbling window.
 * The window is defined with the following range [start, end)
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class TumblingWindow<E extends Timestamped> implements TimeWindow<E>{
    private final long sizeMs;

    private long currentWindowStart = 0;
    private long currentWindowEnd = 0;

    private E startEvent;

    /**
     * @param sizeMs size of the tumbling widow in milliseconds
     */
    public TumblingWindow(long sizeMs){
        this.sizeMs = sizeMs;
    }

    @Override
    public void addEvent(E message) {
        if (message.getTimestamp() > currentWindowEnd) {
            resetWindow(message.getTimestamp());
            this.startEvent = message;
        }
    }

    @Override
    public boolean isInWindow(E event) {
        if(event.getTimestamp() >= currentWindowStart && event.getTimestamp() <= currentWindowEnd){
            return true;
        }

        return false;
    }

    @Override
    public long getWindowStart() {
        return currentWindowStart;
    }

    @Override
    public long getWindowEnd() {
        return currentWindowEnd;
    }



    private void resetWindow(long start){
        this.currentWindowStart = start;
        this.currentWindowEnd = start + (sizeMs -1);
    }


    @Override
    public E getWindowStartEvent() {
        return this.startEvent;
    }
}
