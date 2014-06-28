package ch.uzh.ddis.thesis.lambda_architecture.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.data.IDataFactory;
import ch.uzh.ddis.thesis.lambda_architecture.data.partitioner.HashBucketPartitioner;
import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;
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

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class NettySpout extends BaseRichSpout {
    private static final Logger logger = LogManager.getLogger();

    private final ArrayList<HostAndPort> hosts;
    private final IDataFactory dataFactory;
    private final HashBucketPartitioner partitioner;

    private SpoutOutputCollector outputCollector;
    private TopologyContext context;
    private Map config;
    private NettyClient client;
    private int partitions;

    private ChannelFuture channelFuture;
    private EventLoopGroup workerGroup;

    private NettyHeartbeat nettyHeartbeat;


    public NettySpout(ArrayList<HostAndPort> hosts, IDataFactory dataFactory) {
        this.hosts = hosts;
        this.dataFactory = dataFactory;

        this.partitioner = new HashBucketPartitioner();
        this.client = new NettyClient();
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

        this.partitions = context.getComponentTasks(context.getThisComponentId()).size();

        this.workerGroup = new NioEventLoopGroup();

        this.connect();

        this.nettyHeartbeat = new NettyHeartbeat(this.client);
        Executor executor = Executors.newSingleThreadExecutor();
        executor.execute(nettyHeartbeat);
    }

    private void connect(){
        HostAndPort host = this.hosts.get(context.getThisTaskIndex());

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

            try {
                this.channelFuture = b.connect(host.getHostText(), host.getPort()).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture channelFuture) throws Exception {
                        if (!channelFuture.isSuccess()) {
                            channelFuture.channel().eventLoop().schedule(new Runnable() {
                                @Override
                                public void run() {
                                    logger.debug("reconnecting");
                                    connect();
                                }
                            }, 10, TimeUnit.MILLISECONDS);
                        }
                    }
                }).sync();
            } catch (InterruptedException e){
                logger.error(e);
            }
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    @Override
    public void nextTuple() {
        Optional<String> optionalData = Optional.fromNullable(this.client.getNext());
        if(optionalData.isPresent()) {
            IDataEntry data = this.dataFactory.makeDataEntryFromCSV(optionalData.get());
            int partition = this.partitioner.partition(data.getPartitionKey(), this.partitions);
            this.outputCollector.emit(new Values(data, partition));
        }
    }

    @Override
    public void close() {
        super.close();

        try {
            this.nettyHeartbeat.setFinish(true);
            this.channelFuture.channel().closeFuture().sync();
            this.workerGroup.shutdownGracefully();
        } catch (InterruptedException e){
            logger.error(e);
        }
    }
}
