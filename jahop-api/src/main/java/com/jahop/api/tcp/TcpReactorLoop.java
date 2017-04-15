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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class TcpReactorLoop {
    private static final Logger log = LogManager.getLogger(TcpReactorLoop.class);

    private final AtomicBoolean started = new AtomicBoolean();
    private final RingBuffer<Message> ringBuffer;
    private final SocketAddress serverAddress;
    private final ArrayBlockingQueue<Message> pending;
    private final ArrayList<Message> drained;
    private final ByteBuffer buffer;
    private Thread thread;
    private SocketChannel socketChannel;
    private Selector selector;

    public TcpReactorLoop(final RingBuffer<Message> ringBuffer, final SocketAddress serverAddress) {
        this.ringBuffer = ringBuffer;
        this.serverAddress = serverAddress;
        this.pending = new ArrayBlockingQueue<>(1024);
        this.drained = new ArrayList<>(1024);
        this.buffer = ByteBuffer.allocate(Message.MAX_SIZE);
    }

    void start() {
        if (started.compareAndSet(false, true)) {
            try {
                selector = Selector.open();
                socketChannel = SocketChannel.open(serverAddress);
                socketChannel.configureBlocking(false);
                socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                socketChannel.register(selector, SelectionKey.OP_READ);

                thread = new Thread(this::run);
                thread.setName("tcp-client-thread");
                thread.start();

                log.info("{}: Started", this);
            } catch (IOException e) {
                stop();
                throw new RuntimeException(this + ": Failed to start", e);
            }
        }
    }

    void stop() {
        if (started.compareAndSet(true, false)) {
            try {
                if (selector != null) {
                    selector.close();
                }
                if (thread != null) {
                    thread.join(1000);
                }
            } catch (InterruptedException e) {
                log.error(this + ": Thread interrupted", e);
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                log.error(this + ": Failed to stop gracefully", e);
            } finally {
                try {
                    socketChannel.close();
                } catch (IOException ignore) {
                }
                log.info("{}: Stopped", this);
            }
        }
    }

    public void run() {
        while (started.get()) {
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
        return "TcpReactorLoop{" +
                "serverAddress=" + serverAddress +
                '}';
    }
}