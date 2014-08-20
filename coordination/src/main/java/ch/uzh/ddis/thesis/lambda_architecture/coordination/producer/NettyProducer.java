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
 * The netty producer provides the bootstrap capabilities to communicate with clients from the speed layer
 * that want to access the in-memory message queue.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class NettyProducer extends ChannelInboundHandlerAdapter implements IProducer {
    private static final Logger logger = LogManager.getLogger();

    private static final int readTimeout = 300; // in seconds

    private final int port;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture channelFuture;

    private NettyBuffer buffer;
    public NettyProducer(int port){
        this.port = port;
    }


    @Override
    public void open(){
        this.bossGroup = new NioEventLoopGroup();
        this.workerGroup = new NioEventLoopGroup();

        this.buffer = new NettyBuffer();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new ReadTimeoutHandler(readTimeout));
                            ch.pipeline().addLast(new SimpleNettyHandler(buffer));
                            ch.pipeline().addLast("stringEncoder", new StringEncoder(CharsetUtil.UTF_8));

                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            this.channelFuture = b.bind(this.port).sync();

        }catch (Exception e){
            logger.error(e);
        }
    }

    private void closeSocket(){
        this.channelFuture.channel().closeFuture();
        this.workerGroup.shutdownGracefully();
        this.bossGroup.shutdownGracefully();
    }

    @Override
    public void send(IDataEntry message){
        try {
            this.buffer.getBuffer().put(message.toString());
        } catch (InterruptedException e){
            logger.error(e);
        }
    }

    public void send(String message){
        try {
            this.buffer.getBuffer().put(message);
        } catch (InterruptedException e){
            logger.error(e);
        }
    }

    @Override
    public void close() {
        while(!this.buffer.getBuffer().isEmpty()){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }

        this.closeSocket();
    }
}
