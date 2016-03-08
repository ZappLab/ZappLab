package com.jahop.common.msg;

import java.nio.ByteBuffer;

public abstract class Message {
    private final MsgHeader msgHeader = new MsgHeader();

    public MsgHeader getMsgHeader() {
        return msgHeader;
    }

    public final void read(final ByteBuffer buffer) {
        msgHeader.setMsgType(buffer.getShort());
        msgHeader.setMsgVersion(buffer.getShort());
        msgHeader.setSourceId(buffer.getInt());
        msgHeader.setSeqNo(buffer.getLong());
        msgHeader.setTimestampMs(buffer.getLong());
        readBody(buffer);
    }

    protected abstract void readBody(final ByteBuffer buffer);

    public final void write(final ByteBuffer buffer) {
        buffer.putShort(msgHeader.getMsgType());
        buffer.putShort(msgHeader.getMsgVersion());
        buffer.putInt(msgHeader.getSourceId());
        buffer.putLong(msgHeader.getSeqNo());
        buffer.putLong(msgHeader.getTimestampMs());
        writeBody(buffer);
    }

    protected abstract void writeBody(final ByteBuffer buffer);
}
