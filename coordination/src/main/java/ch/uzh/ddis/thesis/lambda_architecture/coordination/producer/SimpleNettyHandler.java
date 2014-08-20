package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

import com.ecyrd.speed4j.StopWatch;
import com.google.common.base.Optional;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.util.UUID;

/**
 * The netty handler implements the communication protocol of the in-memory message queue.
 * The protocol is pull based. Therefore a client requests a new batch of messages and the producer sends the batch.
 * Note that there is no replay behavior in place. If a batch is dispatched, it is gone forever. If a client
 * closes the connection at the same time a batch is send, the messages are lost.
 *
 * Please see the Netty documentation for a reference on the possible behavior of an inbound handler adapter.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class SimpleNettyHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");

    private final String uuid = UUID.randomUUID().toString();

    private static final int max_buffer_size = 1000;
    private static final int max_batch_size = 150;

    private final NettyBuffer buffer;

    private long processCounter = 0;
    private StopWatch stopWatch;

    public SimpleNettyHandler(NettyBuffer buffer){
        this.buffer = buffer;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);
        Channel channel = ctx.channel();

        if(this.stopWatch == null){
            this.stopWatch = new StopWatch();
        }

        StringBuffer messagePacket = new StringBuffer();
        for(int i = 0; i<max_batch_size; i++){
            Optional<String> optionalData = Optional.fromNullable(buffer.getBuffer().poll());
            if(optionalData.isPresent()){
                messagePacket.append(optionalData.get()).append("$");

                this.processCounter++;
                if(this.processCounter % 1000 == 0){
                    this.stopWatch.stop();
                    logger.info(performance, "topic=nettyThroughput stepSize=1000 duration={}", this.stopWatch.getTimeMicros());
                    this.stopWatch = new StopWatch();
                }
            } else{
                break;
            }
        }

        messagePacket.append("\n");

        channel.writeAndFlush(messagePacket);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        logger.info("channel open {}", uuid);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        logger.debug("channel inactive");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("netty producer caught exception: ", cause);
        cause.printStackTrace();
        ctx.close();
    }

}
