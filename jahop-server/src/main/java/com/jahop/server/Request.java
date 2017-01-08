package com.jahop.server;

import com.jahop.common.msg.Message;
import com.jahop.common.msg.proto.Messages;

import java.nio.channels.SocketChannel;

public class Request {
    private ServerLoop serverLoop;
    private SocketChannel socketChannel;
    private Messages.SnapshotRequest snapshotRequest;
    private Messages.UpdateRequest updateRequest;

    public ServerLoop getServerLoop() {
        return serverLoop;
    }

    public void setServerLoop(ServerLoop serverLoop) {
        this.serverLoop = serverLoop;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void setSocketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    public Messages.SnapshotRequest getSnapshotRequest() {
        return snapshotRequest;
    }

    public void setSnapshotRequest(Messages.SnapshotRequest snapshotRequest) {
        this.snapshotRequest = snapshotRequest;
    }

    public Messages.UpdateRequest getUpdateRequest() {
        return updateRequest;
    }

    public void setUpdateRequest(Messages.UpdateRequest updateRequest) {
        this.updateRequest = updateRequest;
    }

    public boolean isSnapshotRequest() {
        return snapshotRequest != null;
    }

    public void sendResponse(Message response) {
        serverLoop.send(socketChannel, response);
    }

    @Override
    public String toString() {
        return "Request{" +
                "serverLoop=" + serverLoop +
                ", socketChannel=" + socketChannel +
                ", snapshotRequest=" + snapshotRequest +
                ", updateRequest=" + updateRequest +
                '}';
    }
}