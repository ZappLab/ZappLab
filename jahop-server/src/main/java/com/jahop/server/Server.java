package com.jahop.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jahop.common.msg.MessageFactory;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ThreadFactory;

/**
 * Created by Pavel on 8/27/2016.
 */
public class Server {
    private static Logger log = LogManager.getLogger(Server.class);

    private int sourceId = 1;
    private int port = 9090;
    private ServerLoop serverLoop;
    private MessageFactory messageFactory;

    // >>> Server request infrastructure
    private final ThreadFactory workerThreadFactory = new ThreadFactoryBuilder().setNameFormat("worker-thread-%d").build();
    private final RequestFactory requestFactory = new RequestFactory();
    // Specify the size of the request ring buffer, must be power of 2.
    private int requestBufferSize = 1024;
    private Disruptor<Request> requestDisruptor;

    public void setPort(int port) {
        this.port = port;
    }

    public void setSourceId(int sourceId) {
        this.sourceId = sourceId;
    }

    public void setRequestBufferSize(int requestBufferSize) {
        this.requestBufferSize = requestBufferSize;
    }

    public void start() throws IOException {
        messageFactory = new MessageFactory(sourceId);
        // Construct request Disruptor and message handler
        requestDisruptor = new Disruptor<>(requestFactory, requestBufferSize, workerThreadFactory, ProducerType.SINGLE, new BlockingWaitStrategy());
        requestDisruptor.handleEventsWith(new RequestHandler(messageFactory));
        // Start request processing threads
        requestDisruptor.start();

        final RequestProducer producer = new RequestProducer(requestDisruptor.getRingBuffer(), messageFactory);
        serverLoop = new ServerLoop(producer, port);

        serverLoop.start();
    }

    public void stop() {
        requestDisruptor.shutdown();
        serverLoop.stop();
    }
}
