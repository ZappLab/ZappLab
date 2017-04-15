package com.jahop.common.msg;

import java.nio.ByteBuffer;
import java.util.Objects;

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

    public int getMessageSize() {
        return SIZE + bodySize;
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

    void copyFrom(final MessageHeader source) {
        setVersion(source.getVersion());
        setType(source.getType());
        setBodySize(source.getBodySize());
        setSourceId(source.getSourceId());
        setSeqNo(source.getSeqNo());
        setTimestampMs(source.getTimestampMs());
    }

    void clear() {
        setVersion((byte) 0);
        setType((byte) 0);
        setBodySize(0);
        setSourceId(0);
        setSeqNo(0);
        setTimestampMs(0);
    }

    public final boolean read(final ByteBuffer buffer) {
        if (buffer.remaining() < SIZE) {
            return false;
        }
        setVersion(buffer.get());
        setType(buffer.get());
        setBodySize(read2LowerBytes(buffer));
        setSourceId(buffer.getInt());
        setSeqNo(buffer.getLong());
        setTimestampMs(buffer.getLong());
        buffer.getLong();   //reserved
        return true;
    }

    public final boolean write(final ByteBuffer buffer) {
        if (buffer.remaining() < SIZE) {
            return false;
        }
        buffer.put(getVersion());
        buffer.put(getType());
        write2LowerBytes(buffer, getBodySize());
        buffer.putInt(getSourceId());
        buffer.putLong(getSeqNo());
        buffer.putLong(getTimestampMs());
        buffer.putLong(0);  //reserved
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageHeader that = (MessageHeader) o;
        return version == that.version &&
                type == that.type &&
                bodySize == that.bodySize &&
                sourceId == that.sourceId &&
                seqNo == that.seqNo &&
                timestampMs == that.timestampMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, type, bodySize, sourceId, seqNo, timestampMs);
    }

    @Override
    public String toString() {
        return "Hdr{" +
                "ver=" + version +
                ", type=" + MessageType.toString(type) +
                ", bodySz=" + bodySize +
                ", srcId=" + sourceId +
                ", seqNo=" + seqNo +
                ", timeMs=" + timestampMs +
                '}';
    }
}
