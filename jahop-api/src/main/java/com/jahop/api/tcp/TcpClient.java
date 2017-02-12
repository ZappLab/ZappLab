package com.jahop.api.tcp;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jahop.api.Client;
import com.jahop.api.ResponseHandler;
import com.jahop.api.Sender;
import com.jahop.common.msg.Message;
import com.jahop.common.msg.MessageFactory;
import com.jahop.common.util.Sequencer;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ThreadFactory;

public class TcpClient implements Client {
    private static final Logger log = LogManager.getLogger(TcpClient.class);
    private final ThreadFactory workerThreadFactory = new ThreadFactoryBuilder().setNameFormat("worker-thread-%d").build();
    private final Sequencer requestIdSequencer = new Sequencer();
    private final String serverHost;
    private final int serverPort;
    private final int sourceId;
    private final MessageFactory messageFactory;
    private int ringBufferSize = 1024;           // Specify the size of the request ring buffer, must be power of 2.
    private Disruptor<Message> disruptor;
    private ReactorLoop reactorLoop;

    public TcpClient(String serverHost, int serverPort, int sourceId) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.sourceId = sourceId;
        this.messageFactory = new MessageFactory(sourceId, new Sequencer());
    }

    public void setRingBufferSize(int ringBufferSize) {
        this.ringBufferSize = ringBufferSize;
    }

    public void connect() throws IOException {
        disruptor = new Disruptor<>(MessageFactory::allocateMessage, ringBufferSize, workerThreadFactory, ProducerType.SINGLE, new BlockingWaitStrategy());
        disruptor.handleEventsWith(new ResponseHandler());
        // Start request processing threads
        disruptor.start();

        final SocketAddress serverAddress = new InetSocketAddress(serverHost, serverPort);
        reactorLoop = new ReactorLoop(disruptor.getRingBuffer(), serverAddress);
        reactorLoop.start();

        log.info("Client started (remote: {})", serverAddress);
    }

    public void close() throws IOException {
        reactorLoop.stop();
        disruptor.shutdown();
        log.info("Client terminated.");
    }

    @Override
    public Sender getSender(String topic) {
        return data -> {
            final Message payload = messageFactory.createPayload(0, requestIdSequencer.next(), data);
            reactorLoop.send(payload);
            log.info("Sent: {}", payload);
        };
    }
}
