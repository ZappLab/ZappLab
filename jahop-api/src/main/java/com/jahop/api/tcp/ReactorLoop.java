package com.jahop.api.tcp;

import com.jahop.common.msg.Message;
import com.lmax.disruptor.RingBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

public class ReactorLoop {
    private static final Logger log = LogManager.getLogger(ReactorLoop.class);

    private final RingBuffer<Message> ringBuffer;
    private final SocketAddress serverAddress;
    private final ArrayBlockingQueue<Message> pending;
    private final ArrayList<Message> drained;
    private final ByteBuffer buffer;
    private final Thread thread;
    private SocketChannel socketChannel;
    private Selector selector;
    private volatile boolean started;

    public ReactorLoop(final RingBuffer<Message> ringBuffer, final SocketAddress serverAddress) {
        this.ringBuffer = ringBuffer;
        this.serverAddress = serverAddress;
        this.pending = new ArrayBlockingQueue<>(1024);
        this.drained = new ArrayList<>(1024);
        this.buffer = ByteBuffer.allocate(Message.MAX_SIZE);
        this.thread = new Thread(this::run);
        this.thread.setName("reactor-thread");
    }

    public void start() throws IOException {
        initSelector();
        started = true;
        thread.start();
        log.info("Started: {}", this);
    }

    public void run() {
        while (started) {
            try {
                if (!pending.isEmpty()) {
                    final SelectionKey key = socketChannel.keyFor(selector);
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
                        if (key.isReadable()) {
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
            log.error("Reactor thread interrupted.", e);
            Thread.currentThread().interrupt();
        }
        socketChannel.close();
        log.info("Stopped: {}", this);
    }

    private void read(final SelectionKey key) throws IOException {
        // Clear out our read buffer so it's ready for new data
        buffer.clear();

        // Attempt to read off the channel
        int count;
        try {
            count = socketChannel.read(buffer);
        } catch (IOException e) {
            count = -1;
            log.error("Failed to read off the channel", e);
        }

        if (count == -1) {
            log.info("Disconnected from server");
            key.cancel();
            socketChannel.close();
            return;
        }

        buffer.flip();
        if (log.isDebugEnabled()) {
            log.debug("Received {} bytes", count);
        }
        onData();
    }

    private void onData() {
        final long sequence = ringBuffer.next();
        try {
            final Message message = ringBuffer.get(sequence);
            message.read(buffer);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    private void write(SelectionKey key) throws IOException {
        drained.clear();
        pending.drainTo(drained);

        int count = 0;
        for (Message message : drained) {
            buffer.clear();
            if (message.write(buffer)) {
                buffer.flip();
                while (buffer.hasRemaining()) {
                    count += socketChannel.write(buffer);
                }
            } else {
                log.error("Internal buffer filled up");
            }
        }

        key.interestOps(SelectionKey.OP_READ);
        if (log.isDebugEnabled()) {
            log.debug("Sent {} bytes", count);
        }
    }

    private void initSelector() throws IOException {
        selector = SelectorProvider.provider().openSelector();

        socketChannel = SocketChannel.open(serverAddress);
        socketChannel.configureBlocking(false);
        socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        socketChannel.register(selector, SelectionKey.OP_READ);
    }

    public void send(final Message message) {
        try {
            pending.put(message);
            selector.wakeup();
        } catch (InterruptedException e) {
            log.error("Failed to enqueue message: " + message);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return "ReactorLoop{" +
                "serverAddress=" + serverAddress +
                '}';
    }
}