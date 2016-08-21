package com.jahop.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;

public class ServerLoop {
    private static final Logger log = LogManager.getLogger(ServerLoop.class);
    private static final int BUFFER_SIZE = 1024;
    private static final int DEFAULT_PORT = 9090;

    private final ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    private final RequestProducer producer;
    private final int port;
    private final Selector selector;

    public ServerLoop(final RequestProducer producer) throws IOException {
        this(producer, DEFAULT_PORT);
    }

    public ServerLoop(final RequestProducer producer, final int port) throws IOException {
        this.producer = producer;
        this.port = port;
        this.selector = initSelector();
    }

    public void start() {
        log.info("Server started on port: {}", port);
        while (true) {
            try {
                selector.select();
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

            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    private void accept(final SelectionKey key) throws IOException {
        final ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();

        final SocketChannel socketChannel = serverSocketChannel.accept();
        socketChannel.configureBlocking(false);
        socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        socketChannel.register(selector, SelectionKey.OP_READ);

        log.info("Client is connected: {}", socketChannel.getRemoteAddress());
    }

    private void read(final SelectionKey key) throws IOException {
        final SocketChannel socketChannel = (SocketChannel) key.channel();

        // Clear out our read buffer so it's ready for new data
        readBuffer.clear();

        // Attempt to read off the channel
        int numRead;
        try {
            numRead = socketChannel.read(readBuffer);
        } catch (IOException e) {
            socketChannel.close();
            key.cancel();

            log.error("Forceful shutdown");
            return;
        }

        if (numRead == -1) {
            log.info("Graceful shutdown");
            socketChannel.close();
            key.cancel();

            return;
        }

        readBuffer.flip();
        log.info("Available: {} bytes", readBuffer.remaining());
        producer.onData(readBuffer);

//        socketChannel.register(selector, SelectionKey.OP_WRITE);
//
//        numMessages++;
//        if (numMessages % 100000 == 0) {
//            long elapsed = System.currentTimeMillis() - loopTime;
//            loopTime = System.currentTimeMillis();
//        }
    }

    private void write(SelectionKey key) throws IOException {
        final SocketChannel socketChannel = (SocketChannel) key.channel();
        ByteBuffer dummyResponse = ByteBuffer.wrap("ok".getBytes("UTF-8"));

        socketChannel.write(dummyResponse);
        if (dummyResponse.remaining() > 0) {
            log.error("Filled UP");
        }

        key.interestOps(SelectionKey.OP_READ);
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
}