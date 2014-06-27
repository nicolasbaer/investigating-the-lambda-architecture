package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class NettyProducerTest {
    private static final Logger logger = LogManager.getLogger();

    @Test
    public void testRun() throws Exception {
        int port = 8888;
        NettyProducer producer = new NettyProducer(port);
        producer.open();

        for(int i = 0; i < 500; i++){
            producer.send(String.valueOf(i));
        }

        final NettyClient client = new NettyClient();

        String host = "127.0.0.1";
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new StringDecoder(CharsetUtil.UTF_8));
                    ch.pipeline().addLast(client);
                    ch.pipeline().addLast(new StringEncoder(CharsetUtil.UTF_8));
                }
            });

            ChannelFuture f = b.connect(host, port).sync();

            while(client.channel == null){
                Thread.sleep(10);
            }

            client.channel.writeAndFlush("1");

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }

        int counter = 0;
        for(String i : client.received){
            Assert.assertTrue(i.equals(String.valueOf(counter)));
            counter++;
        }

        Assert.assertEquals(client.received.size(), 500);

    }

    private class NettyClient extends ChannelInboundHandlerAdapter {
        ArrayList<String> received = new ArrayList<>(500);
        Channel channel;

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
            logger.debug("channel open");
            this.channel = ctx.channel();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            logger.debug("received response");
            String[] messages = String.valueOf(msg).split("\\n");
            for(String message : messages){
                received.add(message);
            }
            logger.debug("sending new request");
            ctx.channel().writeAndFlush("get new");

        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }


}

