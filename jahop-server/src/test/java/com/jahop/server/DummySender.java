package com.jahop.server;

import com.jahop.common.msg.Message;
import com.jahop.common.msg.MessageFactory;
import com.jahop.common.util.Sequencer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class DummySender {
    private static final Logger log = LogManager.getLogger(DummySender.class);
    private final Sequencer requestIdGenerator = new Sequencer(0);
    private final int sourceId;
    private final SocketAddress serverAddress;
    private final MessageFactory messageFactory;
    private SocketChannel socketChannel;

    public DummySender(final int sourceId, SocketAddress serverAddress) {
        this.sourceId = sourceId;
        this.serverAddress = serverAddress;
        this.messageFactory = new MessageFactory(sourceId, new Sequencer());
    }

    public void connect() throws IOException {
        socketChannel = SocketChannel.open(serverAddress);
        socketChannel.configureBlocking(false);
        //noinspection StatementWithEmptyBody
        while (!socketChannel.finishConnect());
        log.info("DummySender#{}: connected to {}", sourceId, serverAddress);
    }

    public void close() throws IOException {
        socketChannel.close();
        log.info("DummySender#{}: closed", sourceId);
    }

    public void send(final byte[] data) throws IOException {
        send(wrapPayload(data));
    }

    public void send(final ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            final int count = socketChannel.write(buffer);
            log.info("DummySender#{}: sent {} bytes", sourceId, count);
        }
    }

    public ByteBuffer wrapPayload(final byte[] data) {
        final Message message = messageFactory.createPayload(0, requestIdGenerator.next(), data);
        final ByteBuffer buffer = ByteBuffer.allocate(message.getHeader().getMessageSize());
        message.write(buffer);
        buffer.flip();
        return buffer;
    }
}
