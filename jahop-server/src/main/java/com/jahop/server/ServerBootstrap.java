package com.jahop.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jahop.common.msg.MessageFactory;
import com.jahop.common.util.Sequencer;
import com.jahop.server.impl.tcp.TcpConnector;
import com.jahop.server.msg.Request;
import com.jahop.server.msg.RequestFactory;
import com.jahop.server.msg.RequestProducer;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadFactory;

@SpringBootApplication
public class ServerBootstrap {
    private static final Logger log = LogManager.getLogger(ServerBootstrap.class);
    private final ThreadFactory workerThreadFactory = new ThreadFactoryBuilder().setNameFormat("worker-thread-%d").build();
    private final int sourceId = 1;
    private final List<Connector> connectors = connectors();

    private Disruptor<Request> disruptor;

    public static void main(String[] args) {
        SpringApplication.run(ServerBootstrap.class, args);
    }

    @PostConstruct
    public void start() throws IOException {
        final MessageFactory messageFactory = new MessageFactory(sourceId, new Sequencer());
        // Construct request Disruptor and message handler
        disruptor = new Disruptor<>(new RequestFactory(), 1024, workerThreadFactory, ProducerType.SINGLE, new BlockingWaitStrategy());
        disruptor.handleEventsWith(new RequestHandler(messageFactory));
        // Start request processing threads
        disruptor.start();

        final RequestProducer producer = new RequestProducer(disruptor.getRingBuffer());
        connectors.forEach(c -> {
            c.setProducer(producer);
            c.start();
        });
    }

    @PreDestroy
    public void stop() throws IOException {
        connectors.forEach(Connector::stop);
        disruptor.shutdown();
    }

    private List<Connector> connectors() {
        return Arrays.asList(
                new TcpConnector(9090)
        );
    }
}
