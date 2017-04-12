package com.jahop.server.impl.tcp;

import com.jahop.common.msg.Message;
import com.jahop.common.msg.MessageHeader;
import com.jahop.server.*;
import com.jahop.server.msg.RequestProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Collection;
import java.util.Iterator;

import static com.jahop.server.Errors.SYSTEM_TCP_CONNECTOR;

public class TcpConnector implements Connector {
    private static final Logger log = LogManager.getLogger(TcpConnector.class);
    private final MessagesQueue messagesQueue = new MessagesQueue();
    private final MessageHeader header = new MessageHeader();
    private final SocketAddress serverAddress;

    private RequestProducer producer;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private Thread thread;
    private volatile boolean started;

    public TcpConnector(final int port) {
        this.serverAddress = new InetSocketAddress(port);
    }

    @Override
    public void setProducer(RequestProducer producer) {
        this.producer = producer;
    }

    @Override
    public void start() {
        try {
            selector = Selector.open();

            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.socket().bind(serverAddress);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            throw new ServerException(SYSTEM_TCP_CONNECTOR, "Failed to start " + this, e);
        }

        thread = new Thread(this::run);
        thread.setName("tcp-connector-thread");

        started = true;
        thread.start();
        log.info("Started: {}", this);
    }

    @Override
    public void stop() {
        messagesQueue.clear();
        started = false;
        try {
            selector.close();
            thread.join(1000);
            serverSocketChannel.close();
        } catch (Exception e) {
            throw new ServerException(SYSTEM_TCP_CONNECTOR, "Failed to stop gracefully " + this, e);
        }
        log.info("Stopped: {}", this);
    }

    @Override
    public void send(final Source source, final Message message) {
        messagesQueue.pushMessage(source, message);
        selector.wakeup();
    }

    private void run() {
        while (started) {
            try {
                final Collection<Source> pendingSources = messagesQueue.drainSources();
                for (Source source : pendingSources) {
                    final TcpSource tcpSource = (TcpSource) source;
                    final SelectionKey key = tcpSource.getSocketChannel().keyFor(selector);
                    key.interestOps(SelectionKey.OP_WRITE);
                }

                if (selector.select() > 0) {
                    final Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
                    while (selectedKeys.hasNext()) {
                        final SelectionKey key = selectedKeys.next();
                        selectedKeys.remove();

                        if (!key.isValid()) {
                            continue;
                        }

                        // Check what event is available and deal with it
                        if (key.isAcceptable()) {
                            accept(key);
                        } else if (key.isReadable()) {
                            read(key);
                        } else if (key.isWritable()) {
                            write(key);
                        }
                    }
                }
            } catch (Exception e) {
                throw new ServerException(SYSTEM_TCP_CONNECTOR, "Fatal error " + this, e);
            }
        }
    }

    private void accept(final SelectionKey key) throws IOException {
        final ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();

        final SocketChannel socketChannel = serverChannel.accept();
        final Source source = new TcpSource(socketChannel.getRemoteAddress().toString(), this, socketChannel);
        socketChannel.configureBlocking(false);
        socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        socketChannel.register(selector, SelectionKey.OP_READ, source);
        messagesQueue.registerSource(source);
        log.info("{}: connected", source);
    }

    private void read(final SelectionKey key) throws IOException {
        final TcpSource source = (TcpSource) key.attachment();
        final SocketChannel socketChannel = source.getSocketChannel();
        final ByteBuffer buffer = source.getInBuffer();

        try {
            final int count = socketChannel.read(buffer);
            if (count == -1) {
                throw new ClosedChannelException();
            }
            if (log.isDebugEnabled()) {
                log.debug("{}: received {} bytes", source, count);
            }

            buffer.flip();
            buffer.mark();
            while (buffer.hasRemaining()) {
                if (!header.read(buffer) || header.getBodySize() > buffer.remaining()) {
                    break;
                }
                producer.onData(source, header, buffer);
                buffer.mark();
            }
            buffer.reset();
            buffer.compact();
            if (log.isDebugEnabled() && buffer.hasRemaining()) {
                log.debug("{}: waiting for remaining data (received {} bytes)", source, count);
            }
        } catch (IOException e) {
            if (e.getMessage() != null) {
                log.error("{}: disconnected, reason - '{}'", source, e.getMessage());
            } else {
                log.info("{}: disconnected", source);
            }
            messagesQueue.unregisterSource(source);
            source.close();
            key.cancel();
        }
    }

    private void write(SelectionKey key) throws IOException {
        final TcpSource source = (TcpSource) key.attachment();
        final SocketChannel socketChannel = source.getSocketChannel();
        final ByteBuffer buffer = source.getOutBuffer();

        if (!buffer.hasRemaining()) {
            buffer.clear();
            final Collection<Message> messages = messagesQueue.drainMessages(source);
            for (Message message : messages) {
                if (!message.write(buffer)) {
                    log.error("{}: internal buffer filled up", source);
                }
            }
            buffer.flip();
        }

        int count = 0;
        if (buffer.hasRemaining()) {
            count += socketChannel.write(buffer);
        }

        if (buffer.hasRemaining()) {
            key.interestOps(SelectionKey.OP_WRITE);
        } else {
            key.interestOps(SelectionKey.OP_READ);
        }

        if (log.isDebugEnabled()) {
            log.debug("{}: sent {} bytes", source, count);
        }
    }

    @Override
    public String toString() {
        return "TcpConnector{" +
                "address=" + serverAddress +
                '}';
    }
}