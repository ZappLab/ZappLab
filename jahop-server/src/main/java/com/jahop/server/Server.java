package com.jahop.server;

import com.jahop.common.msg.Message;
import com.jahop.common.msg.MessageHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

public class Server {
    private static final Logger log = LogManager.getLogger(Server.class);
    private final HashMap<SocketChannel, ByteBuffer> inBuffers = new HashMap<>(16);
    private final ByteBuffer outBuffer = ByteBuffer.allocate(Message.MAX_SIZE);
    private final SocketChannels socketChannels = new SocketChannels();
    private final MessageHeader header = new MessageHeader();
    private final RequestProducer producer;
    private final int port;
    private final Selector selector;
    private final Thread thread;
    private volatile boolean started;

    public Server(final RequestProducer producer, final int port) throws IOException {
        this.producer = producer;
        this.port = port;
        this.selector = initSelector();
        this.thread = new Thread(this::run);
        this.thread.setName("server-thread");
    }

    public void start() {
        started = true;
        thread.start();
        log.info("Started: {}", this);
    }

    public void run() {
        while (started) {
            try {
                final Collection<SocketChannel> pendingChannels = socketChannels.drainPendingChannels();
                for (SocketChannel channel : pendingChannels) {
                    final SelectionKey key = channel.keyFor(selector);
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
                log.error("Fatal error.", e);
                System.exit(1);
            }
        }
    }

    public void stop() throws IOException {
        started = false;
        selector.close();
        try {
            thread.join(1000);
        } catch (InterruptedException e) {
            log.error("Server thread interrupted.", e);
            Thread.currentThread().interrupt();
        }
        log.info("Stopped: {}", this);
    }

    private void accept(final SelectionKey key) throws IOException {
        final ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();

        final SocketChannel socketChannel = serverChannel.accept();
        socketChannel.configureBlocking(false);
        socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        socketChannel.register(selector, SelectionKey.OP_READ);
        socketChannels.registerChannel(socketChannel);
        log.info("{}: connected", socketChannel.getRemoteAddress());
    }

    private void read(final SelectionKey key) throws IOException {
        final SocketChannel socketChannel = (SocketChannel) key.channel();
        final SocketAddress remoteAddress = socketChannel.getRemoteAddress();
        final ByteBuffer buffer = inBuffers.computeIfAbsent(socketChannel, socketChannel1 -> ByteBuffer.allocate(Message.MAX_SIZE));

        try {
            final int count = socketChannel.read(buffer);
            if (count == -1) {
                throw new ClosedChannelException();
            }
            if (log.isDebugEnabled()) {
                log.debug("{}: received {} bytes", remoteAddress, count);
            }

            buffer.flip();
            buffer.mark();
            while (header.read(buffer)) {
                if (buffer.remaining() < header.getBodySize()) {
                    buffer.reset();
                    break;
                }
                producer.onData(this, socketChannel, header, buffer);
                buffer.mark();
            }
            buffer.compact();
        } catch (IOException e) {
            if (e.getMessage() != null) {
                log.error("{}: disconnected, reason - '{}'", remoteAddress, e.getMessage());
            } else {
                log.info("{}: disconnected", remoteAddress);
            }
            socketChannels.unregisterChannel(socketChannel);
            inBuffers.remove(socketChannel);
            key.cancel();
            socketChannel.close();
        }
    }

    private void write(SelectionKey key) throws IOException {
        final SocketChannel socketChannel = (SocketChannel) key.channel();
        final SocketAddress remoteAddress = socketChannel.getRemoteAddress();
        final Collection<Message> messages = socketChannels.drainMessages(socketChannel);

        int count = 0;
        for (Message message : messages) {
            outBuffer.clear();
            if (message.write(outBuffer)) {
                outBuffer.flip();
                while (outBuffer.hasRemaining()) {
                    count += socketChannel.write(outBuffer);
                }
            } else {
                log.error("{}: internal buffer filled up", remoteAddress);
            }
        }

        key.interestOps(SelectionKey.OP_READ);
        if (log.isDebugEnabled()) {
            log.debug("{}: sent {} bytes", remoteAddress, count);
        }
    }

    private Selector initSelector() throws IOException {
        final Selector socketSelector = SelectorProvider.provider().openSelector();

        final ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);

        final InetSocketAddress isa = new InetSocketAddress(port);
        serverChannel.socket().bind(isa);
        serverChannel.register(socketSelector, SelectionKey.OP_ACCEPT);
        return socketSelector;
    }

    public void send(final SocketChannel channel, final Message message) {
        socketChannels.putMessage(channel, message);
        selector.wakeup();
    }

    @Override
    public String toString() {
        return "Server{" +
                "port=" + port +
                '}';
    }
}