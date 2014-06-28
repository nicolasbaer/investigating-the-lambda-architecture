package ch.uzh.ddis.thesis.lambda_architecture.storm.spout;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class NettyClient extends ChannelInboundHandlerAdapter implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger logger = LogManager.getLogger();

    private static final int max_buffer = 5000;

    private final ArrayBlockingQueue<String> queue;

    private long lastDataReceived = 0;
    private Channel channel;

    public NettyClient() {
        this.queue = new ArrayBlockingQueue<>(max_buffer);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        this.lastDataReceived = System.currentTimeMillis();
        String[] messages = String.valueOf(msg).split("\\n");
        for(String message : messages){
            queue.put(message);
        }
        ctx.channel().writeAndFlush("next");
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);

        this.channel = ctx.channel();

        // initiate data transfer
        ctx.channel().writeAndFlush("next");

    }

    /**
     * Retrieves the next data item.
     * @return null if no data is available otherwise the data of course :)
     */
    public String getNext(){
        return this.queue.poll();
    }


    public long getLastDataReceived() {
        return lastDataReceived;
    }

    public ArrayBlockingQueue<String> getQueue() {
        return queue;
    }

    public Channel getChannel() {
        return channel;
    }
}
