package com.jahop.common.msg;

import java.nio.ByteBuffer;

/**
 * Common header for all messages
 * Size: 32 bytes
 * 8 bytes are reserved for future use
 */

public final class MsgHeader {
    public static final int SIZE = 32;  //Header size is 24 bytes
    private short msgType;              //  pos=0, size=2
    private short msgVersion;           //  pos=2, size=2
    private int sourceId;               //  pos=4, size=4
    private long seqNo;                 //  pos=8, size=8
    private long timestampMs;           //  pos=16, size=8
//    private long reserved;              //  pos=24, size=8

    public short getMsgType() {
        return msgType;
    }

    public void setMsgType(short msgType) {
        this.msgType = msgType;
    }

    public short getMsgVersion() {
        return msgVersion;
    }

    public void setMsgVersion(short msgVersion) {
        this.msgVersion = msgVersion;
    }

    public int getSourceId() {
        return sourceId;
    }

    public void setSourceId(int sourceId) {
        this.sourceId = sourceId;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public void setSeqNo(long seqNo) {
        this.seqNo = seqNo;
    }

    public long getTimestampMs() {
        return timestampMs;
    }

    public void setTimestampMs(long timestampMs) {
        this.timestampMs = timestampMs;
    }

    public final void read(final ByteBuffer buffer) {
        setMsgType(buffer.getShort());
        setMsgVersion(buffer.getShort());
        setSourceId(buffer.getInt());
        setSeqNo(buffer.getLong());
        setTimestampMs(buffer.getLong());
        buffer.getLong();   //reserved
    }

    public final void write(final ByteBuffer buffer) {
        buffer.putShort(getMsgType());
        buffer.putShort(getMsgVersion());
        buffer.putInt(getSourceId());
        buffer.putLong(getSeqNo());
        buffer.putLong(getTimestampMs());
        buffer.putLong(0);  //reserved
    }

    @Override
    public String toString() {
        return "MsgHeader{" +
                "msgType=" + MsgType.toString(msgType) +
                ", msgVersion=" + msgVersion +
                ", sourceId=" + sourceId +
                ", seqNo=" + seqNo +
                ", timestampMs=" + timestampMs +
                '}';
    }
}
