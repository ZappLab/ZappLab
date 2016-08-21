package com.jahop.api;

import com.jahop.common.msg.MsgType;
import com.jahop.common.msg.Payload;
import com.jahop.common.msg.proto.Messages;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

public class Client {
    private static final Logger log = LogManager.getLogger(Client.class);
    private final ByteBuffer sendBuffer = ByteBuffer.allocate(Payload.MAX_SIZE);
    private final Payload payload = new Payload();
    private final AtomicLong sequencer = new AtomicLong(System.currentTimeMillis());
    private String host;
    private int port;
    private SocketChannel socketChannel;

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        final SocketAddress serverAddress = new InetSocketAddress(host, port);
        socketChannel = SocketChannel.open(serverAddress);
        socketChannel.configureBlocking(false);
        socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        log.info("Client started (remote: {})", serverAddress);
    }

    public void stop() throws IOException {
        socketChannel.close();
        log.info("Client terminated.");
    }

    public void send(final Messages.SnapshotRequest request) throws IOException {
        sendBuffer.clear();
        final long timestamp = System.currentTimeMillis();
        payload.getMsgHeader().setMsgType(MsgType.SNAPSHOT_REQUEST);
        payload.getMsgHeader().setMsgVersion((short) 1);
        payload.getMsgHeader().setSourceId(42);
        payload.getMsgHeader().setSeqNo(sequencer.incrementAndGet());
        payload.getMsgHeader().setTimestampMs(timestamp);
        payload.setPartNo(0);
        payload.setPartsCount(1);
        final byte[] data = request.toByteArray();
        payload.setPartSize(data.length);
        payload.write(sendBuffer);
        sendBuffer.put(data);
        sendBuffer.flip();
        socketChannel.write(sendBuffer);
        log.info("Send: {}", payload);
    }
}
