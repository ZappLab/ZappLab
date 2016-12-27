package com.jahop.server;

import com.jahop.common.msg.Message;
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

    private final ByteBuffer readBuffer = ByteBuffer.allocate(Message.MAX_SIZE);
    private final RequestProducer producer;
    private final int port;
    private final Selector selector;

    private final Thread thread;
    private volatile boolean started;

    public ServerLoop(final RequestProducer producer, final int port) throws IOException {
        this.producer = producer;
        this.port = port;
        this.selector = initSelector();
        this.thread = new Thread(this::run);
        this.thread.setName("server-thread");
    }

    public void start() {
        started = true;
        thread.start();
        log.info("Server started on port: {}", port);
    }

    public void run() {
        while (started) {
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
                log.error("Fatal error.", e);
                System.exit(1);
            }
        }
    }

    public void stop() {
        started = false;
        try {
            thread.join(1000);
        } catch (InterruptedException e) {
            log.error("Server thread interrupted.", e);
        }
        log.info("Server stopped");
    }

    private void accept(final SelectionKey key) throws IOException {
        final ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();

        final SocketChannel socketChannel = serverSocketChannel.accept();
        socketChannel.configureBlocking(false);
        socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        socketChannel.register(selector, SelectionKey.OP_READ);

        log.info("{}: connected", socketChannel.getRemoteAddress());
    }

    private void read(final SelectionKey key) throws IOException {
        final SocketChannel socketChannel = (SocketChannel) key.channel();

        // Clear out our read buffer so it's ready for new data
        readBuffer.clear();

        // Attempt to read off the channel
        final int numRead;
        try {
            numRead = socketChannel.read(readBuffer);
        } catch (IOException e) {
            log.error("{}: disconnected ({})", socketChannel.getRemoteAddress(), e.getMessage());
            key.cancel();
            socketChannel.close();
            return;
        }

        if (numRead == -1) {
            log.info("{}: disconnected", socketChannel.getRemoteAddress());
            key.cancel();
            socketChannel.close();
            return;
        }

        readBuffer.flip();
        if (readBuffer.hasRemaining()) {
            log.info("{}: received {} bytes", socketChannel.getRemoteAddress(), readBuffer.remaining());
            producer.onData(readBuffer);
        }
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