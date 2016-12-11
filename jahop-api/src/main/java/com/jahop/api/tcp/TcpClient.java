package com.jahop.api.tcp;

import com.jahop.api.Client;
import com.jahop.api.Sender;
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

public class TcpClient implements Client {
    private static final Logger log = LogManager.getLogger(TcpClient.class);
    private final ByteBuffer sendBuffer = ByteBuffer.allocate(Payload.MAX_SIZE);
    private final Payload payload = new Payload();
    private final AtomicLong sequencer = new AtomicLong(System.currentTimeMillis());
    private final String serverHost;
    private final int serverPort;
    private SocketChannel socketChannel;

    public TcpClient(String serverHost, int serverPort) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
    }

    public void connect() throws IOException {
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
        return new Sender() {
            @Override
            public void send(byte[] data) {
                try {
                    sendBuffer.clear();
                    final long timestamp = System.currentTimeMillis();
                    payload.getMsgHeader().setMsgType(MsgType.SNAPSHOT_REQUEST);
                    payload.getMsgHeader().setMsgVersion((short) 1);
                    payload.getMsgHeader().setSourceId(42);
                    payload.getMsgHeader().setSeqNo(sequencer.incrementAndGet());
                    payload.getMsgHeader().setTimestampMs(timestamp);
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
            }
        };
    }
}
