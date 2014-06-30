package ch.uzh.ddis.thesis.lambda_architecture.speed.spout;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class NettyHeartbeat implements Runnable{
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");
    private static final Marker remoteDebug = MarkerManager.getMarker("DEBUGFLUME");

    private final static long heartBeatRate = 500;

    private final NettyClient client;

    private boolean finish = false;

    public NettyHeartbeat(NettyClient client) {
        this.client = client;
    }

    @Override
    public void run() {
        while(!finish){
            long lastData = this.client.getLastDataReceived();
            if(lastData != 0 && (System.currentTimeMillis() - lastData) > heartBeatRate && this.client.getQueue().isEmpty()){
                if(this.client.getChannel().isActive()){
                    this.client.getChannel().writeAndFlush("next");
                    logger.info(remoteDebug, "topic=nettyHeartbeat lastData={}", lastData);
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
