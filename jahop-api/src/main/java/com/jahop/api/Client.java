package com.jahop.api;

import com.jahop.common.msg.MsgType;
import com.jahop.common.msg.Payload;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

public class Client {
    private final ByteBuffer sendBuffer = ByteBuffer.allocate(1024);
    private final Payload payload = new Payload();
    private final AtomicLong sequencer = new AtomicLong(System.currentTimeMillis());
    private final SocketAddress serverAddress;
    private final int port;
    private SocketChannel socketChannel;

    public Client(InetAddress serverAddress, int port) {
        this.serverAddress = new InetSocketAddress(serverAddress, port);
        this.port = port;
    }

    public void start() throws IOException {
        socketChannel = SocketChannel.open(serverAddress);
        socketChannel.configureBlocking(false);
        socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
    }

    public void stop() throws IOException {
        socketChannel.close();
    }

    public void send(final byte[] data) throws IOException {
        sendBuffer.clear();
        final long timestamp = System.currentTimeMillis();
        payload.getMsgHeader().setMsgType(MsgType.SNAPSHOT_REQUEST);
        payload.getMsgHeader().setMsgVersion((short) 1);
        payload.getMsgHeader().setSourceId(42);
        payload.getMsgHeader().setSeqNo(sequencer.incrementAndGet());
        payload.getMsgHeader().setTimestampMs(timestamp);
        payload.setPartNo(0);
        payload.setPartsCount(1);
        payload.setSize(data.length);
        payload.setData(ByteBuffer.wrap(data));
        payload.write(sendBuffer);
        sendBuffer.flip();
        socketChannel.write(sendBuffer);
        System.out.println(payload);
    }

    public static void main(String[] args) throws Exception {
        final Client client = new Client(InetAddress.getLocalHost(), 9090);
        client.start();

        for (byte i = 0; i < 8; i++) {
            client.send(new byte[] {i});
        }
        Thread.sleep(1000);
        client.stop();
    }
}
