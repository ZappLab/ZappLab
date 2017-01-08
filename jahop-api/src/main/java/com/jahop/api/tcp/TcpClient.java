package com.jahop.api.tcp;

import com.jahop.api.Client;
import com.jahop.api.Sender;
import com.jahop.common.msg.MessageFactory;
import com.jahop.common.msg.MessageType;
import com.jahop.common.msg.Payload;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class TcpClient implements Client {
    private static final Logger log = LogManager.getLogger(TcpClient.class);
    private final ByteBuffer sendBuffer = ByteBuffer.allocate(Payload.MAX_SIZE);
    private final String serverHost;
    private final int serverPort;
    private final int sourceId;
    private MessageFactory messageFactory;
    private SocketChannel socketChannel;

    public TcpClient(String serverHost, int serverPort, int sourceId) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.sourceId = sourceId;
    }

    public void connect() throws IOException {
        messageFactory = new MessageFactory(sourceId);
        final SocketAddress serverAddress = new InetSocketAddress(serverHost, serverPort);
        socketChannel = SocketChannel.open(serverAddress);
        socketChannel.configureBlocking(false);
        socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        log.info("Client started (remote: {})", serverAddress);
    }

    public void close() throws IOException {
        socketChannel.close();
        log.info("Client terminated.");
    }

    @Override
    public Sender getSender(String topic) {
        return data -> {
            try {
                sendBuffer.clear();
                final Payload payload = messageFactory.createPayload(MessageType.SNAPSHOT_REQUEST);
                payload.setRequestId(777);
                payload.setPartNo(0);
                payload.setPartsCount(1);
                payload.setPartSize(data.length);
                payload.write(sendBuffer);
                sendBuffer.put(data);
                sendBuffer.flip();
                socketChannel.write(sendBuffer);
                log.info("Send: {}", payload);
            } catch (IOException e) {
                log.error("Failed to send", e);
            }
        };
    }
}
