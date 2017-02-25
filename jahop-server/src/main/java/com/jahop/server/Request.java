package com.jahop.server;

import com.jahop.common.msg.Message;
import com.jahop.common.msg.MessageHeader;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Request {
    enum Status {OK, ERROR}
    private final Message message;
    private Server server;
    private SocketChannel socketChannel;
    private Status status;

    public Request(Message message) {
        this.message = message;
    }

    public void setServer(Server server) {
        this.server = server;
    }

    public void setSocketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    public Message getMessage() {
        return message;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void sendResponse(Message response) {
        server.send(socketChannel, response);
    }

    public void read(final MessageHeader header, final ByteBuffer buffer) {
        message.getHeader().copyFrom(header);
        status = message.readBody(buffer) ? Status.OK : Status.ERROR;
    }

    public boolean isReady() {
        return status == Status.OK;
    }
}