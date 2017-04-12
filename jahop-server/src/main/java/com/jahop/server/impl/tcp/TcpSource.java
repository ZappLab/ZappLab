package com.jahop.server.impl.tcp;

import com.jahop.common.msg.Message;
import com.jahop.server.Connector;
import com.jahop.server.Errors;
import com.jahop.server.ServerException;
import com.jahop.server.Source;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class TcpSource implements Source{
    private final ByteBuffer inBuffer = ByteBuffer.allocate(Message.MAX_SIZE);
    private final ByteBuffer outBuffer = ByteBuffer.allocate(Message.MAX_SIZE);

    private final String name;
    private final Connector connector;
    private final SocketChannel socketChannel;

    public TcpSource(String name, Connector connector, SocketChannel socketChannel) {
        this.name = name;
        this.connector = connector;
        this.socketChannel = socketChannel;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public ByteBuffer getInBuffer() {
        return inBuffer;
    }

    public ByteBuffer getOutBuffer() {
        return outBuffer;
    }

    @Override
    public void send(Message message) {
        connector.send(this, message);
    }

    @Override
    public void close() {
        try {
            socketChannel.close();
        } catch (IOException e) {
            throw new ServerException(Errors.SYSTEM_TCP_CONNECTOR, "Failed to close " + this, e);
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
