package com.jahop.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jahop.common.msg.MessageFactory;
import com.jahop.common.util.Sequencer;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;

/**
 * Created by Pavel on 8/27/2016.
 */
public class ServerBootstrap {
    private static Logger log = LogManager.getLogger(ServerBootstrap.class);

    private final ThreadFactory workerThreadFactory = new ThreadFactoryBuilder().setNameFormat("worker-thread-%d").build();

    private int sourceId = 1;
    private int port = 9090;
    private int ringBufferSize = 1024;           // Specify the size of the request ring buffer, must be power of 2.

    private Server server;
    private MessageFactory messageFactory;
    private Disruptor<Request> disruptor;

    public void setPort(int port) {
        this.port = port;
    }

    public void setSourceId(int sourceId) {
        this.sourceId = sourceId;
    }

    public void setRingBufferSize(int ringBufferSize) {
        this.ringBufferSize = ringBufferSize;
    }

    public void start() throws IOException {
        messageFactory = new MessageFactory(sourceId, new Sequencer());
        // Construct request Disruptor and message handler
        disruptor = new Disruptor<>(new RequestFactory(), ringBufferSize, workerThreadFactory, ProducerType.SINGLE, new BlockingWaitStrategy());
        disruptor.handleEventsWith(new RequestHandler(messageFactory));
        // Start request processing threads
        disruptor.start();

        final RequestProducer producer = new RequestProducer(disruptor.getRingBuffer());
        server = new Server(producer, new InetSocketAddress(port));
        server.start();
    }

    public void stop() throws IOException {
        server.stop();
        disruptor.shutdown();
    }
}
