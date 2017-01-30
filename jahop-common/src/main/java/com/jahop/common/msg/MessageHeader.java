package com.jahop.common.msg;

import java.nio.ByteBuffer;

import static com.jahop.common.util.ByteUtils.read2LowerBytes;
import static com.jahop.common.util.ByteUtils.write2LowerBytes;

/**
 * Common header for all messages
 * Size: 32 bytes
 * 8 bytes are reserved for future use
 */

public final class MessageHeader {
    public static final int SIZE = 32;      //Header size is 32 bytes

    private byte version;                   //  pos=0, size=1
    private byte type;                      //  pos=1, size=1
    private int bodySize;                   //  pos=2, size=2
    private int sourceId;                   //  pos=4, size=4
    private long seqNo;                     //  pos=8, size=8
    private long timestampMs;               //  pos=16, size=8
//    private long reserved;                 //  pos=24, size=8

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public int getBodySize() {
        return bodySize;
    }

    public void setBodySize(int bodySize) {
        this.bodySize = bodySize;
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

    public void clear() {
        setVersion((byte) 0);
        setType((byte) 0);
        setBodySize(0);
        setSourceId(0);
        setSeqNo(0);
        setTimestampMs(0);
    }

    public final void read(final ByteBuffer buffer) {
        setVersion(buffer.get());
        setType(buffer.get());
        setBodySize(read2LowerBytes(buffer));
        setSourceId(buffer.getInt());
        setSeqNo(buffer.getLong());
        setTimestampMs(buffer.getLong());
        buffer.getLong();   //reserved
    }

    public final void write(final ByteBuffer buffer) {
        buffer.put(getVersion());
        buffer.put(getType());
        write2LowerBytes(buffer, getBodySize());
        buffer.putInt(getSourceId());
        buffer.putLong(getSeqNo());
        buffer.putLong(getTimestampMs());
        buffer.putLong(0);  //reserved
    }

    @Override
    public String toString() {
        return "MessageHeader{" +
                "version=" + version +
                ", type=" + MessageType.toString(type) +
                ", bodySize=" + bodySize +
                ", sourceId=" + sourceId +
                ", seqNo=" + seqNo +
                ", timestampMs=" + timestampMs +
                '}';
    }
}
