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
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.jahop.server.Errors.SYSTEM_TCP_CONNECTOR;

public class TcpConnector implements Connector {
    private static final Logger log = LogManager.getLogger(TcpConnector.class);
    private final AtomicBoolean started = new AtomicBoolean();
    private final MessagesQueue messagesQueue = new MessagesQueue();
    private final MessageHeader header = new MessageHeader();
    private final SocketAddress serverAddress;

    private RequestProducer producer;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private Thread thread;

    public TcpConnector(final int port) {
        this.serverAddress = new InetSocketAddress(port);
    }

    @Override
    public void setProducer(RequestProducer producer) {
        this.producer = producer;
    }

    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            try {
                selector = Selector.open();
                serverSocketChannel = ServerSocketChannel.open();
                serverSocketChannel.configureBlocking(false);
                serverSocketChannel.socket().bind(serverAddress);
                serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

                thread = new Thread(this::run);
                thread.setName("tcp-connector-thread");
                thread.start();

                log.info("{}: Started", this);
            } catch (IOException e) {
                stop();
                throw new ServerException(SYSTEM_TCP_CONNECTOR, this + ": Failed to start", e);
            }
        }
    }

    @Override
    public void stop() {
        if (started.compareAndSet(true, false)) {
            try {
                messagesQueue.clear();
                if (selector != null) {
                    selector.close();
                }
                if (thread != null) {
                    thread.join(1000);
                }
            } catch (Exception e) {
                throw new ServerException(SYSTEM_TCP_CONNECTOR, this + ": Failed to stop gracefully", e);
            } finally {
                try {
                    serverSocketChannel.close();
                } catch (IOException ignore) {
                }
                log.info("{}: Stopped", this);
            }
        }
    }

    @Override
    public void send(final Source source, final Message message) {
        if (!started.get()) {
            throw new ServerException(SYSTEM_TCP_CONNECTOR, this + ": Not started");
        }
        messagesQueue.addLast(source, message);
        selector.wakeup();
    }

    private void run() {
        while (started.get()) {
            try {
                messagesQueue.drainSources(source -> {
                    final SelectionKey key = ((TcpSource) source).getSocketChannel().keyFor(selector);
                    if (key != null) {
                        key.interestOps(SelectionKey.OP_WRITE);
                    } else {
                        disconnect(source, null, null);
                    }
                });

                if (selector.select() > 0 && selector.isOpen()) {
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
            } catch (IOException e) {
                throw new ServerException(SYSTEM_TCP_CONNECTOR, this + ": Fatal error", e);
            }
        }
    }

    private void accept(final SelectionKey key) {
        final ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();

        try {
            final SocketChannel socketChannel = serverChannel.accept();
            final Source source = new TcpSource(socketChannel.getRemoteAddress().toString(), this, socketChannel);
            socketChannel.configureBlocking(false);
            socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
            socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            socketChannel.register(selector, SelectionKey.OP_READ, source);
            messagesQueue.registerSource(source);
            log.info("{}: connected", source);
        } catch (IOException e) {
            log.error(this + ": Accept connection failed", e);
        }
    }

    private void read(final SelectionKey key) {
        final TcpSource source = (TcpSource) key.attachment();
        final SocketChannel socketChannel = source.getSocketChannel();
        final ByteBuffer buffer = source.getInBuffer();

        try {
            final int count = socketChannel.read(buffer);
            if (log.isDebugEnabled()) {
                log.debug("{}: received {} bytes", source, count);
            }
            if (count == -1) {
                throw new ClosedChannelException();
            } else if (count > 0) {
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
            }
        } catch (IOException e) {
            disconnect(source, key, e);
        }
    }

    private void write(SelectionKey key) {
        final TcpSource source = (TcpSource) key.attachment();
        final SocketChannel socketChannel = source.getSocketChannel();
        final ByteBuffer buffer = source.getOutBuffer();

        try {
            int count = 0;
            if (buffer.hasRemaining()) {
                count = socketChannel.write(buffer);
            }

            if (!buffer.hasRemaining()) {
                final Message message = messagesQueue.pollMessage(source);
                if (message != null) {
                    buffer.clear();
                    if (!message.write(buffer)) {
                        buffer.clear();
                        log.error("{}: out buffer overflow. Skipping {}", source, message);
                    }
                    buffer.flip();
                }
            }

            key.interestOps(buffer.hasRemaining() ? SelectionKey.OP_WRITE : SelectionKey.OP_READ);

            if (log.isDebugEnabled()) {
                log.debug("{}: sent {} bytes", source, count);
            }
        } catch (IOException e) {
            disconnect(source, key, e);
        }
    }

    private void disconnect(final Source source, final SelectionKey key, final Exception e) {
        messagesQueue.unregisterSource(source);
        source.close();
        if (key != null) {
            key.cancel();
        }
        if (e != null && e.getMessage() != null) {
            log.error("{}: disconnected, reason - '{}'", source, e.getMessage());
        } else {
            log.info("{}: disconnected", source);
        }
    }

    @Override
    public String toString() {
        return "TcpConnector[" + serverAddress + ']';
    }
}