package com.jahop.common.msg;

import java.nio.ByteBuffer;

public final class Ack extends Message {
    public static final int SIZE = MessageHeader.SIZE + 8;
    private long requestId;

    public long getRequestId() {
        return requestId;
    }

    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }

    @Override
    protected void readBody(ByteBuffer buffer) {
        requestId = buffer.getLong();
    }

    @Override
    protected void writeBody(ByteBuffer buffer) {
        buffer.putLong(requestId);
    }

    @Override
    protected int getSize() {
        return SIZE;
    }

    @Override
    public String toString() {
        return "Ack{" +
                "header=" + getMsgHeader() +
                ", requestId=" + requestId +
                '}';
    }
}
