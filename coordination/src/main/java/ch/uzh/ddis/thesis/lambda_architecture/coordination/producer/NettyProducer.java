package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.CharsetUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class NettyProducer extends ChannelInboundHandlerAdapter implements IProducer, Runnable {
    private static final Logger logger = LogManager.getLogger();

    private static final int readTimeout = 5; // in seconds

    private final int port;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture channelFuture;

    private SimpleNettyHandler handler;


    public NettyProducer(int port){
        this.port = port;
    }

    @Override
    public void open(){
        this.bossGroup = new NioEventLoopGroup();
        this.workerGroup = new NioEventLoopGroup();

        this.handler = new SimpleNettyHandler();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new ReadTimeoutHandler(readTimeout));
                            ch.pipeline().addLast(handler);
                            ch.pipeline().addLast("stringEncoder", new StringEncoder(CharsetUtil.UTF_8));

                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            this.channelFuture = b.bind(this.port).sync();
        }catch (InterruptedException e){
            logger.error(e);
        }
    }

    private void processMessages(){


        this.closeSocket();
    }

    private void closeSocket(){
        try {
            this.channelFuture.channel().closeFuture().sync();
            this.workerGroup.shutdownGracefully();
            this.bossGroup.shutdownGracefully();
        } catch (InterruptedException e) {
            logger.error(e);
        }
    }

    @Override
    public void send(IDataEntry message){
        try {
            this.handler.getQueue().put(message.toString());
        } catch (InterruptedException e){
            logger.error(e);
        }
    }

    public void send(String message){
        try {
            this.handler.getQueue().put(message);
        } catch (InterruptedException e){
            logger.error(e);
        }
    }

    @Override
    public void close() {
        while(!this.handler.getQueue().isEmpty()){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }

        this.closeSocket();
    }

    @Override
    public void run() {
        this.processMessages();
    }
}
