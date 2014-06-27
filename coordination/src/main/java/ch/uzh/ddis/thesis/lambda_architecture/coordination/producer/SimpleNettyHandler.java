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

import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class SimpleNettyHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");

    private static final int max_buffer_size = 1000;
    private static final int max_batch_size = 250;

    private final ArrayBlockingQueue<String> queue;

    private long processCounter = 0;
    private StopWatch stopWatch;

    public SimpleNettyHandler(){
        this.queue = new ArrayBlockingQueue<>(max_buffer_size);
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
            Optional<String> optionalData = Optional.fromNullable(queue.poll());
            if(optionalData.isPresent()){
                messagePacket.append(optionalData.get()).append("\n");

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

        channel.writeAndFlush(messagePacket);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        logger.debug("channel open");
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        logger.debug("channel inactive");
    }

    public ArrayBlockingQueue<String> getQueue() {
        return queue;
    }
}
