package com.jahop.common.msg;

import java.nio.ByteBuffer;

public class Ack extends Message {
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
    public String toString() {
        return "Ack{" +
                "requestId=" + requestId +
                '}';
    }
}
