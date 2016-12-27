package com.jahop.common.msg;

import java.nio.ByteBuffer;

/**
 * Common header for all messages
 * Size: 32 bytes
 * 8 bytes are reserved for future use
 */

public final class MessageHeader {
    public static final int SIZE = 32;      //Header size is 24 bytes
    private short type;                     //  pos=0, size=2
    private short version;                  //  pos=2, size=2
    private int sourceId;                   //  pos=4, size=4
    private long seqNo;                     //  pos=8, size=8
    private long timestampMs;               //  pos=16, size=8
//    private long reserved;                //  pos=24, size=8

    public short getType() {
        return type;
    }

    public void setType(short type) {
        this.type = type;
    }

    public short getVersion() {
        return version;
    }

    public void setVersion(short version) {
        this.version = version;
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
        setType(buffer.getShort());
        setVersion(buffer.getShort());
        setSourceId(buffer.getInt());
        setSeqNo(buffer.getLong());
        setTimestampMs(buffer.getLong());
        buffer.getLong();   //reserved
    }

    public final void write(final ByteBuffer buffer) {
        buffer.putShort(getType());
        buffer.putShort(getVersion());
        buffer.putInt(getSourceId());
        buffer.putLong(getSeqNo());
        buffer.putLong(getTimestampMs());
        buffer.putLong(0);  //reserved
    }

    @Override
    public String toString() {
        return "MessageHeader{" +
                "type=" + MessageType.toString(type) +
                ", version=" + version +
                ", sourceId=" + sourceId +
                ", seqNo=" + seqNo +
                ", timestampMs=" + timestampMs +
                '}';
    }
}
