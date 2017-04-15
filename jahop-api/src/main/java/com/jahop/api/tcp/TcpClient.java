package com.jahop.api.tcp;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.GeneratedMessage;
import com.jahop.api.Client;
import com.jahop.api.ResponseHandler;
import com.jahop.common.msg.Message;
import com.jahop.common.msg.MessageFactory;
import com.jahop.common.util.Sequencer;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.SocketAddress;
import java.util.concurrent.ThreadFactory;

public class TcpClient implements Client {
    private static final Logger log = LogManager.getLogger(TcpClient.class);
    private final ThreadFactory workerThreadFactory = new ThreadFactoryBuilder().setNameFormat("worker-thread-%d").build();
    private final Sequencer requestIdSequencer = new Sequencer();
    private final SocketAddress serverAddress;
    private final int sourceId;
    private final MessageFactory messageFactory;
    private int ringBufferSize = 1024;           // Specify the size of the request ring buffer, must be power of 2.
    private Disruptor<Message> disruptor;
    private TcpReactorLoop reactorLoop;

    public TcpClient(final SocketAddress serverAddress, final int sourceId) {
        this.serverAddress = serverAddress;
        this.sourceId = sourceId;
        this.messageFactory = new MessageFactory(sourceId, new Sequencer());
    }

    public void setRingBufferSize(int ringBufferSize) {
        this.ringBufferSize = ringBufferSize;
    }

    @Override
    public void start() {
        // start event handler thread
        disruptor = new Disruptor<>(MessageFactory::allocateMessage, ringBufferSize, workerThreadFactory, ProducerType.SINGLE, new BlockingWaitStrategy());
        disruptor.handleEventsWith(new ResponseHandler());
        disruptor.start();

        // start tcp connector thread
        reactorLoop = new TcpReactorLoop(disruptor.getRingBuffer(), serverAddress);
        reactorLoop.start();

        log.info("Client started (remote: {})", serverAddress);
    }

    @Override
    public void stop() {
        reactorLoop.stop();
        disruptor.shutdown();
        log.info("Client stopped");
    }

    @Override
    public void send(final GeneratedMessage message) {
        final Message payload = messageFactory.createPayload(0, requestIdSequencer.next(), message.toByteArray());
        reactorLoop.send(payload);
        log.info("Sent: {}", payload);
    }
}
