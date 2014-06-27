package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

import java.util.ArrayList;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class NettyProducerFactory implements IProducerFactory{

    private final static Integer startPort = 5050;

    private ArrayList<Integer> ports;


    public NettyProducerFactory() {
        this.ports = new ArrayList<>();
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

        return producer;
    }
}
