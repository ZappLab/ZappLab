package com.jahop.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jahop.common.msg.MessageFactory;
import com.jahop.common.util.Sequencer;
import com.jahop.server.connectors.Connectors;
import com.jahop.server.connectors.tcp.TcpConnector;
import com.jahop.server.handlers.EchoRequestHandler;
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
import java.util.concurrent.ThreadFactory;

@SpringBootApplication
public class ServerBootstrap {
    private static final Logger log = LogManager.getLogger(ServerBootstrap.class);
    private static final int SOURCE_ID = 1;
    private final ThreadFactory workerThreadFactory = new ThreadFactoryBuilder().setNameFormat("worker-thread-%d").build();
    private Disruptor<Request> disruptor;
    private Connectors connectors;

    public static void main(String[] args) {
        SpringApplication.run(ServerBootstrap.class, args);
    }

    @PostConstruct
    public void start() throws IOException {
        final MessageFactory messageFactory = new MessageFactory(SOURCE_ID, new Sequencer());

        disruptor = new Disruptor<>(new RequestFactory(), 1024, workerThreadFactory, ProducerType.SINGLE, new BlockingWaitStrategy());

        final RequestProducer producer = new RequestProducer(disruptor.getRingBuffer());
        connectors = new Connectors(new TcpConnector(9090, producer));
        disruptor.handleEventsWith(new EchoRequestHandler(connectors, messageFactory));

        disruptor.start();
        connectors.start();

        log.info("Server started");
    }

    @PreDestroy
    public void stop() throws IOException {
        connectors.stop();
        disruptor.shutdown();
        log.info("Stopped started");
    }
}
