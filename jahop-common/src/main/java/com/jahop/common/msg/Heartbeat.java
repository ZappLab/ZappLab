package com.jahop.common.msg;

import java.nio.ByteBuffer;

public class Heartbeat extends Message {
    private long revision;

    public long getRevision() {
        return revision;
    }

    public void setRevision(long revision) {
        this.revision = revision;
    }

    @Override
    protected void readBody(ByteBuffer buffer) {
        revision = buffer.getLong();
    }

    @Override
    protected void writeBody(ByteBuffer buffer) {
        buffer.putLong(revision);
    }

    @Override
    public String toString() {
        return "Heartbeat{" +
                "revision=" + revision +
                '}';
    }
}
