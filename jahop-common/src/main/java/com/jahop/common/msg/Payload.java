package com.jahop.common.msg;

import java.nio.ByteBuffer;

import static com.jahop.common.util.ByteUtils.read2LowerBytes;
import static com.jahop.common.util.ByteUtils.write2LowerBytes;

/**
 * Payload message. Contains binary data.
 * If reference data is larger than max partSize, it is uploaded using multiple payload messages.
 * Size: 32 bytes (header) + 16 bytes (payload header) + data.length
 */
public class Payload extends Message {
    public static final int PAYLOAD_HEADER_SIZE = 16;
    public static final int MAX_PART_SIZE = MAX_SIZE - (MessageHeader.SIZE + PAYLOAD_HEADER_SIZE);
    private long requestId;
    private int partsCount;
    private int partNo;
    private int partSize;

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

    public int getPartSize() {
        return partSize;
    }

    public void setPartSize(int partSize) {
        this.partSize = partSize;
    }

    @Override
    protected void readBody(ByteBuffer buffer) {
        requestId = buffer.getLong();
        partsCount = read2LowerBytes(buffer);
        partNo = read2LowerBytes(buffer);
        partSize = read2LowerBytes(buffer);
        buffer.get();    //padding
        buffer.get();    //padding
    }

    @Override
    protected void writeBody(ByteBuffer buffer) {
        buffer.putLong(requestId);
        write2LowerBytes(buffer, partsCount);
        write2LowerBytes(buffer, partNo);
        write2LowerBytes(buffer, partSize);
        buffer.put((byte)0);   //padding
        buffer.put((byte)0);   //padding
    }

    @Override
    protected int getSize() {
        return MessageHeader.SIZE + PAYLOAD_HEADER_SIZE;
    }

    @Override
    public String toString() {
        return "Payload{" +
                "header=" + getHeader() +
                ", requestId=" + requestId +
                ", partsCount=" + partsCount +
                ", partNo=" + partNo +
                ", partSize=" + partSize +
                '}';
    }
}
