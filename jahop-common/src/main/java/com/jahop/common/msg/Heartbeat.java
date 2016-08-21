package com.jahop.common.msg;

import java.nio.ByteBuffer;

public final class Heartbeat extends Message {
    public static final int SIZE = MsgHeader.SIZE + 8;
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
    protected int getSize() {
        return SIZE;
    }

    @Override
    public String toString() {
        return "Heartbeat{" +
                "header=" + getMsgHeader() +
                ", revision=" + revision +
                '}';
    }
}
