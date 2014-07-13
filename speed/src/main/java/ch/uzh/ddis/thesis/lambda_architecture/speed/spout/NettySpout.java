package ch.uzh.ddis.thesis.lambda_architecture.speed.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.data.IDataFactory;
import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class NettySpout extends BaseRichSpout {
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");
    private static final Marker remoteDebug = MarkerManager.getMarker("DEBUGFLUME");

    private final ArrayList<HostAndPort> hosts;
    private final IDataFactory dataFactory;

    private SpoutOutputCollector outputCollector;
    private TopologyContext context;
    private Map config;

    private ChannelFuture channelFuture;
    private EventLoopGroup workerGroup;

    private boolean finished = false;

    private final NettyQueue nettyQueue = new NettyQueue();


    public NettySpout(ArrayList<HostAndPort> hosts, IDataFactory dataFactory) {
        this.hosts = hosts;
        this.dataFactory = dataFactory;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        Fields fields = new Fields("data", "partition");
        outputFieldsDeclarer.declare(fields);
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.outputCollector = spoutOutputCollector;
        this.config = conf;
        this.context = topologyContext;

        this.workerGroup = new NioEventLoopGroup();

        this.connect();
    }

    private void connect(){
        HostAndPort host = this.hosts.get(context.getThisTaskIndex());

        Bootstrap b = new Bootstrap();
        b.group(workerGroup);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast("frameDecoder", new LineBasedFrameDecoder(120 * 150));
                ch.pipeline().addLast(new StringDecoder(CharsetUtil.UTF_8));
                ch.pipeline().addLast(new NettyClient(nettyQueue));
                ch.pipeline().addLast(new StringEncoder(CharsetUtil.UTF_8));
            }
        });

        try {
            this.channelFuture = b.connect(host.getHostText(), host.getPort()).sync();
            this.channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    if (!finished && !channelFuture.isSuccess() && !channelFuture.channel().isOpen()) {
                        channelFuture.channel().eventLoop().schedule(new Runnable() {
                            @Override
                            public void run() {
                                connect();
                            }
                        }, 10, TimeUnit.MILLISECONDS);
                    }
                }
            });

        } catch (InterruptedException e){
            logger.error(e);
        }

    }

    private long lastTs = 0;

    @Override
    public void nextTuple() {
        Optional<String> optionalData = Optional.fromNullable(this.nettyQueue.queue.poll());
        if(optionalData.isPresent()) {
            IDataEntry data = this.dataFactory.makeDataEntryFromCSV(optionalData.get());
            this.outputCollector.emit(new Values(data, data.getPartitionKey()));
            if(lastTs!=0){
                if(data.getTimestamp() < lastTs){
                    logger.info(remoteDebug, "topic=spoutTimeIssue lastTs={} currentTs={}", lastTs, data.getTimestamp());
                }
            }
            lastTs = data.getTimestamp();
        }
    }

    @Override
    public void close() {
        logger.info("close spout called");
        super.close();
        this.finished = true;

        try {
            this.channelFuture.channel().closeFuture().sync();
            this.workerGroup.shutdownGracefully();
        } catch (InterruptedException e){
            logger.error(e);
        }
    }
}
