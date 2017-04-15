package com.jahop.api.tcp;

import com.jahop.common.msg.Message;
import com.jahop.common.msg.MessageHeader;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class TcpReactorLoop {
    private static final Logger log = LogManager.getLogger(TcpReactorLoop.class);

    private final ByteBuffer rcvBuffer = (ByteBuffer) ByteBuffer.allocate(Message.MAX_SIZE).clear();
    private final ByteBuffer sndBuffer = (ByteBuffer) ByteBuffer.allocate(Message.MAX_SIZE).flip();
    private final ArrayBlockingQueue<Message> pending = new ArrayBlockingQueue<>(1024);
    private final AtomicBoolean started = new AtomicBoolean();
    private final MessageHeader header = new MessageHeader();

    private final RingBuffer<Message> ringBuffer;
    private final SocketAddress serverAddress;

    private Thread thread;
    private SocketChannel socketChannel;
    private Selector selector;
    private SelectionKey selectionKey;

    public TcpReactorLoop(final RingBuffer<Message> ringBuffer, final SocketAddress serverAddress) {
        this.ringBuffer = ringBuffer;
        this.serverAddress = serverAddress;
    }

    void start() {
        if (started.compareAndSet(false, true)) {
            try {
                selector = Selector.open();
                socketChannel = SocketChannel.open(serverAddress);
                socketChannel.configureBlocking(false);
                socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                selectionKey = socketChannel.register(selector, SelectionKey.OP_READ);

                thread = new Thread(this::run);
                thread.setName("tcp-client-thread");
                thread.start();

                log.info(this + ": Started");
            } catch (IOException e) {
                stop();
                throw new RuntimeException(this + ": " + e.getMessage(), e);
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
                log.error(this + ": Interrupted");
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                log.error(this + ": " + e.getMessage(), e);
            } finally {
                try {
                    if (socketChannel != null) {
                        socketChannel.close();
                    }
                } catch (IOException ignore) {
                }
                log.info(this + ": Stopped");
            }
        }
    }

    public void run() {
        while (started.get()) {
            try {
                if (!pending.isEmpty()) {
                    selectionKey.interestOps(SelectionKey.OP_WRITE);
                }

                if (selector.select() > 0 && selector.isOpen() && selectionKey.isValid()) {
                    // Check what event is available and deal with it
                    if (selectionKey.isReadable()) {
                        read();
                    } else if (selectionKey.isWritable()) {
                        write();
                    }
                }
            } catch (IOException e) {
                log.error("Fatal error", e);
            }
        }
    }

    private void read() {
        // Attempt to read off the channel
        int count = -1;
        try {
            count = socketChannel.read(rcvBuffer);
        } catch (IOException e) {
            log.error("Failed to read off the channel", e);
        }
        if (log.isDebugEnabled()) {
            log.debug("Received {} bytes", count);
        }

        if (count == -1) {
            throw new RuntimeException("Connection closed");
        }
        if (count > 0) {
            rcvBuffer.flip();
            rcvBuffer.mark();
            while (rcvBuffer.hasRemaining()) {
                if (!header.read(rcvBuffer) || header.getBodySize() > rcvBuffer.remaining()) {
                    break;
                }
                onData();
                rcvBuffer.mark();
            }
            rcvBuffer.reset();
            rcvBuffer.compact();
        }
    }

    private void onData() {
        final long sequence = ringBuffer.next();
        try {
            ringBuffer.get(sequence).read(header, rcvBuffer);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    private void write() {
        int count = 0;
        if (sndBuffer.hasRemaining()) {
            try {
                count = socketChannel.write(sndBuffer);
            } catch (IOException e) {
                log.error("Failed to write to channel", e);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Sent {} bytes", count);
        }

        if (!sndBuffer.hasRemaining()) {
            final Message message = pending.poll();
            sndBuffer.clear();
            if (!message.write(sndBuffer)) {
                sndBuffer.clear();
                log.error("Send buffer overflow. Skipping {}", message);
            }
            sndBuffer.flip();
        }

        selectionKey.interestOps(sndBuffer.hasRemaining() ? SelectionKey.OP_WRITE : SelectionKey.OP_READ);
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
        return "TcpReactorLoop[" + serverAddress + ']';
    }
}