package com.jahop.server;

import com.jahop.common.msg.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collection;
import java.util.Iterator;

public class Server {
    private static final Logger log = LogManager.getLogger(Server.class);

    private final RequestProducer producer;
    private final int port;
    private final ByteBuffer buffer;
    private final Selector selector;
    private final ConnectionManager connectionManager;
    private final Thread thread;
    private volatile boolean started;

    public Server(final RequestProducer producer, final int port) throws IOException {
        this.producer = producer;
        this.port = port;
        this.buffer = ByteBuffer.allocate(Message.MAX_SIZE);
        this.selector = initSelector();
        this.connectionManager = new ConnectionManager();
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
                final Collection<SocketChannel> pendingChannels = connectionManager.pollPendingChannels();
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

        final SocketChannel channel = serverChannel.accept();
        channel.configureBlocking(false);
        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        channel.register(selector, SelectionKey.OP_READ);
        connectionManager.addChannel(channel);
        log.info("{}: connected", channel.getRemoteAddress());
    }

    private void read(final SelectionKey key) throws IOException {
        final SocketChannel channel = (SocketChannel) key.channel();
        final SocketAddress remoteAddress = channel.getRemoteAddress();

        // Clear out our read buffer so it's ready for new data
        buffer.clear();

        // Attempt to read off the channel
        int count;
        try {
            count = channel.read(buffer);
        } catch (IOException e) {
            count = -1;
            log.error(remoteAddress + ": failed to read off the channel", e);
        }

        if (count == -1) {
            log.info("{}: disconnected", remoteAddress);
            connectionManager.removeChannel(channel);
            key.cancel();
            channel.close();
            return;
        }

        buffer.flip();
        if (log.isDebugEnabled()) {
            log.debug("{}: received {} bytes", remoteAddress, count);
        }
        producer.onData(this, channel, buffer);
    }

    private void write(SelectionKey key) throws IOException {
        final SocketChannel channel = (SocketChannel) key.channel();
        final SocketAddress remoteAddress = channel.getRemoteAddress();

        final Collection<Message> messages = connectionManager.pollChannelMessages(channel);
        int count = 0;
        for (Message message : messages) {
            buffer.clear();
            if (message.write(buffer)) {
                buffer.flip();
                count += channel.write(buffer);
                if (buffer.remaining() > 0) {
                    log.error("{}: socket buffer filled up", remoteAddress);
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
        connectionManager.addChannelMessage(channel, message);
        selector.wakeup();
    }

    @Override
    public String toString() {
        return "Server{" +
                "port=" + port +
                '}';
    }
}