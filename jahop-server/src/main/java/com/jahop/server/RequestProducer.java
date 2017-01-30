package com.jahop.server;

import com.jahop.common.msg.Message;
import com.jahop.common.msg.MessageFactory;
import com.lmax.disruptor.RingBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

class RequestProducer {
    private static final Logger log = LogManager.getLogger(RequestProducer.class);

    private final RingBuffer<Request> ringBuffer;
    private final MessageFactory messageFactory;

    RequestProducer(final RingBuffer<Request> ringBuffer, MessageFactory messageFactory) {
        this.ringBuffer = ringBuffer;
        this.messageFactory = messageFactory;
    }

    void onData(Server server, SocketChannel socketChannel, final ByteBuffer buffer) throws IOException {
//        while (buffer.hasRemaining())
        {
            final long sequence = ringBuffer.next();
            try {
                final Request request = ringBuffer.get(sequence);
                request.setServer(server);
                request.setSocketChannel(socketChannel);
                final Message message = request.getMessage();
                message.read(buffer);
                server.send(socketChannel, messageFactory.createAck(message.getRequestId()));
            } finally {
                ringBuffer.publish(sequence);
            }
        }
    }
}
