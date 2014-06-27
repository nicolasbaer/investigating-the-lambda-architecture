package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

import java.util.ArrayList;
import java.util.concurrent.Executor;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class NettyProducerFactory implements IProducerFactory{

    private final static Integer startPort = 5050;
    private final Executor executor;

    private ArrayList<Integer> ports;


    public NettyProducerFactory(Executor executor) {
        this.ports = new ArrayList<>();
        this.executor = executor;
    }

    @Override
    public NettyProducer makeProducer() {
        int port;
        if(ports.isEmpty()){
            port = startPort;
        }else{
            port = ports.get(ports.size()-1);
        }

        this.ports.add(port);

        NettyProducer producer = new NettyProducer(port);
        producer.open();
        executor.execute(producer);

        return producer;
    }
}
