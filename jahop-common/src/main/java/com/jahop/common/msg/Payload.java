package com.jahop.common.msg;

import com.jahop.common.util.ByteUtils;

import java.nio.ByteBuffer;

import static com.jahop.common.util.ByteUtils.read2LowerBytes;
import static com.jahop.common.util.ByteUtils.write2LowerBytes;

/**
 * Payload message. Contains binary data (~64Kb max)
 * If reference data is larger than max size, it is uploaded using multiple payload messages.
 * Size: 32 bytes (header) + 16 bytes (payload header) + data.length
 */
public class Payload extends Message {
    public static final int MAX_SIZE = 1 << 16;     // 64k
    public static final int MAX_DATA_SIZE = MAX_SIZE - (MsgHeader.SIZE + 16);
    private long requestId;
    private int partNo;
    private int partsCount;
    private int size;

    public long getRequestId() {
        return requestId;
    }

    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }

    public int getPartNo() {
        return partNo;
    }

    public void setPartNo(int partNo) {
        this.partNo = partNo;
    }

    public int getPartsCount() {
        return partsCount;
    }

    public void setPartsCount(int partsCount) {
        this.partsCount = partsCount;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    protected void readBody(ByteBuffer buffer) {
        requestId = buffer.getLong();
        partNo = read2LowerBytes(buffer);
        partsCount = read2LowerBytes(buffer);
        size = read2LowerBytes(buffer);
        buffer.get();    //padding
        buffer.get();    //padding
    }

    @Override
    protected void writeBody(ByteBuffer buffer) {
        buffer.putLong(requestId);
        write2LowerBytes(buffer, partNo);
        write2LowerBytes(buffer, partsCount);
        write2LowerBytes(buffer, size);
        buffer.put((byte)0);   //padding
        buffer.put((byte)0);   //padding
    }

    @Override
    public String toString() {
        return "Payload{" +
                "msgHeader=" + getMsgHeader() +
                ", requestId=" + requestId +
                ", partNo=" + partNo +
                ", partsCount=" + partsCount +
                ", size=" + size +
                '}';
    }
}
