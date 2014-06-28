package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class NettyBuffer {

    private final ArrayBlockingQueue<String> buffer;

    public NettyBuffer() {
        this.buffer = new ArrayBlockingQueue<String>(500);
    }

    public ArrayBlockingQueue<String> getBuffer() {
        return buffer;
    }
}
