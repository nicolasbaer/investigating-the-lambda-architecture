package ch.uzh.ddis.thesis.lambda_architecture.speed.spout;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.util.UUID;

/**
 * The heartbeat checks if the netty client has reached a timeout and stopped consuming messages from the coordination
 * layer. If that happens the heartbeat will resume the consumer.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class NettyHeartbeat implements Runnable{
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");
    private static final Marker remoteDebug = MarkerManager.getMarker("DEBUGFLUME");

    private final static long heartBeatRate = 5;
    private final String uuid = UUID.randomUUID().toString();

    private final NettyClient client;
    private final NettyQueue nettyQueue;

    private boolean finish = false;

    public NettyHeartbeat(NettyClient client, NettyQueue nettyQueue) {
        this.client = client;
        this.nettyQueue = nettyQueue;
    }

    @Override
    public void run() {
        while(!finish){
            long lastData = this.client.getLastDataReceived();
            if(lastData != 0 && (System.currentTimeMillis() - lastData) > heartBeatRate && this.nettyQueue.queue.isEmpty()){
                if(this.client.getChannel().isActive()){
                    this.client.getChannel().writeAndFlush("next");
                    logger.info(remoteDebug, "topic=nettyHeartbeat lastData={} uuid={} clientid={}", lastData, uuid, this.client.uuid);
                }
            }

            try {
                Thread.sleep(heartBeatRate);
            } catch (InterruptedException e){
                logger.error(e);
            }
        }

    }

    public void setFinish(boolean finish) {
        this.finish = finish;
    }
}
