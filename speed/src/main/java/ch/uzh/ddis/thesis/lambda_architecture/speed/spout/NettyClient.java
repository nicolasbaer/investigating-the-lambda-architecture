package ch.uzh.ddis.thesis.lambda_architecture.speed.spout;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.io.Serializable;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class NettyClient extends ChannelInboundHandlerAdapter implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");
    private static final Marker remoteDebug = MarkerManager.getMarker("DEBUGFLUME");

    private final String uuid = UUID.randomUUID().toString();

    private static final int max_buffer = 5000;

    private final NettyQueue nettyQueue;

    private long lastDataReceived = 0;
    private Channel channel;

    public NettyClient(NettyQueue queue) {
        this.nettyQueue = queue;
    }



    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        this.lastDataReceived = System.currentTimeMillis();

        String[] lines = String.valueOf(msg).split(Pattern.quote("$"));
        for(String line : lines){
            if(!line.equals("")) {
                this.nettyQueue.queue.put(line);
            }
        }

        ctx.channel().writeAndFlush("next");
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);

        this.channel = ctx.channel();

        // initiate data transfer
        this.channel.writeAndFlush("next");

    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        logger.debug("channel inactive...");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        logger.error(cause);
    }



    public long getLastDataReceived() {
        return lastDataReceived;
    }

    public Channel getChannel() {
        return channel;
    }
}
