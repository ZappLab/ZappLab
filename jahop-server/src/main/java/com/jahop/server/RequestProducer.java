package com.jahop.server;

import com.jahop.common.msg.MessageHeader;
import com.lmax.disruptor.RingBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

class RequestProducer {
    private static final Logger log = LogManager.getLogger(RequestProducer.class);

    private final RingBuffer<Request> ringBuffer;

    RequestProducer(final RingBuffer<Request> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    void onData(final Server server, final SocketChannel socketChannel, final MessageHeader header, final ByteBuffer buffer) {
        final long sequence = ringBuffer.next();
        try {
            final Request request = ringBuffer.get(sequence);
            request.setServer(server);
            request.setSocketChannel(socketChannel);
            request.read(header, buffer);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}
