package com.jahop.server;

import com.jahop.common.msg.Message;

import java.nio.channels.SocketChannel;

public class Request {
    private Server server;
    private SocketChannel socketChannel;
    private Message message;

    public Server getServer() {
        return server;
    }

    public void setServer(Server server) {
        this.server = server;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void setSocketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public void sendResponse(Message response) {
        server.send(socketChannel, response);
    }

}