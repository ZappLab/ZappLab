package com.jahop.server.connectors.tcp;

import com.jahop.common.msg.Message;
import com.jahop.common.msg.MessageProvider;
import com.jahop.common.msg.MessageHeader;
import com.jahop.common.msg.MessageType;
import com.jahop.common.util.Sequencer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;

public class DummyClient {
    private static final Logger log = LogManager.getLogger(DummyClient.class);
    private final Sequencer requestIdGenerator = new Sequencer(0);
    private final ByteBuffer buffer = ByteBuffer.allocate(Message.MAX_SIZE);
    private final Message message = MessageProvider.allocateMessage();
    private final int sourceId;
    private final SocketAddress serverAddress;
    private final MessageProvider messageProvider;
    private SocketChannel socketChannel;
    private SelectionKey selectionKey;

    public DummyClient(final int sourceId, final SocketAddress serverAddress) {
        this.sourceId = sourceId;
        this.serverAddress = serverAddress;
        this.messageProvider = new MessageProvider(sourceId, 0);
    }

    public int getSourceId() {
        return sourceId;
    }

    public void connect(final Selector selector) throws IOException {
        socketChannel = SocketChannel.open(serverAddress);
        if (selector != null) {
            socketChannel.configureBlocking(false);
            selectionKey = socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, this);
        }
        log.info("DummyClient#{}: connected to {}", sourceId, serverAddress);
    }

    public void close() throws IOException {
        selectionKey.cancel();
        socketChannel.close();
        log.info("DummyClient#{}: closed", sourceId);
    }

    public long send(final ByteBuffer... buffer) throws IOException {
        final long count = socketChannel.write(buffer);
        if (count > 0) {
            log.info("DummyClient#{}: sent {} bytes", sourceId, count);
        }
        return count;
    }

    public int receive(final Consumer<Message> consumer) throws IOException {
        final int count = socketChannel.read(buffer);
        if (count == -1) {
            close();
        } else {
            buffer.flip();
            buffer.mark();
            while (message.read(buffer)) {
                buffer.mark();
                consumer.accept(message);
            }
            buffer.reset();
            buffer.compact();
        }
        return count;
    }

    ByteBuffer wrapPayload(final byte[] data) {
        final Message message = messageProvider.createPayload(0, requestIdGenerator.next(), data);
        final ByteBuffer buffer = ByteBuffer.allocate(message.getHeader().getMessageSize());
        message.write(buffer);
        buffer.flip();
        return buffer;
    }
}
