package com.jahop.server;

import com.jahop.common.msg.Message;
import com.jahop.common.msg.MessageFactory;
import com.jahop.common.util.Sequencer;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class DummySender {
    private final Sequencer requestIdGenerator = new Sequencer(0);
    private final MessageFactory messageFactory;
    private final SocketAddress serverAddress;
    private SocketChannel socketChannel;

    public DummySender(final int sourceId, SocketAddress serverAddress) {
        this.serverAddress = serverAddress;
        this.messageFactory = new MessageFactory(sourceId, new Sequencer());
    }

    public void connect() throws IOException {
        socketChannel = SocketChannel.open(serverAddress);
        socketChannel.configureBlocking(false);
        while (!socketChannel.finishConnect());

    }

    public void close() throws IOException {
        socketChannel.close();
    }

    public void send(final byte[] data) throws IOException {
        final Message message = messageFactory.createPayload(0, requestIdGenerator.next(), data);
        final ByteBuffer buffer = ByteBuffer.allocate(message.getHeader().getMessageSize());
        message.write(buffer);
        buffer.flip();

        while (buffer.hasRemaining()) {
            socketChannel.write(buffer);
        }
    }
}
