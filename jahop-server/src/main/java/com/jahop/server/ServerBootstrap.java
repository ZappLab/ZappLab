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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadFactory;

@SpringBootApplication
public class ServerBootstrap {
    private static final Logger log = LogManager.getLogger(ServerBootstrap.class);
    private static final int SOURCE_ID = 1;
    private final ThreadFactory workerThreadFactory = new ThreadFactoryBuilder().setNameFormat("worker-thread-%d").build();

    private Disruptor<Request> disruptor;
    private List<Connector> connectors;

    public static void main(String[] args) {
        SpringApplication.run(ServerBootstrap.class, args);
    }

    @PostConstruct
    public void start() throws IOException {
        final MessageFactory messageFactory = new MessageFactory(SOURCE_ID, new Sequencer());
        // Construct request Disruptor and message handler
        disruptor = new Disruptor<>(new RequestFactory(), 1024, workerThreadFactory, ProducerType.SINGLE, new BlockingWaitStrategy());
        disruptor.handleEventsWith(new RequestHandler(messageFactory));
        // Start request processing threads
        disruptor.start();

        final RequestProducer producer = new RequestProducer(disruptor.getRingBuffer());
        connectors = connectors(producer);
        connectors.forEach(Connector::start);
        log.info("Server started");
    }

    @PreDestroy
    public void stop() throws IOException {
        connectors.forEach(Connector::stop);
        disruptor.shutdown();
        log.info("Stopped started");
    }

    private List<Connector> connectors(final RequestProducer producer) {
        final TcpConnector connector = new TcpConnector(9090);
        connector.setProducer(producer);
        return Collections.singletonList(connector);
    }
}
