package com.jahop.api;

import com.jahop.common.msg.Request;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Client {
    private final ByteBuffer sendBuffer = ByteBuffer.allocate(16);
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

    public void send(final Request request) throws IOException {
        sendBuffer.clear();
        sendBuffer.putLong(0, request.getSeqNo());
        sendBuffer.putLong(8, request.getSourceId());
        socketChannel.write(sendBuffer);
    }

    public static void main(String[] args) throws Exception {
        final Client client = new Client(InetAddress.getLocalHost(), 9090);
        client.start();

        for (int i = 0; i < 10; i++) {
            final Request request = new Request();
            request.setSeqNo(i);
            request.setSourceId(42);
            client.send(request);
        }
        Thread.sleep(1000);
        client.stop();
    }
}
