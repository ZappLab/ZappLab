package com.jahop.common.msg;

import java.nio.ByteBuffer;

public class Payload extends Message {
    private int partNo;
    private int partsCount;
    private int size;
    private ByteBuffer data;

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

    public ByteBuffer getData() {
        return data;
    }

    public void setData(ByteBuffer data) {
        this.data = data;
    }

    @Override
    protected void readBody(ByteBuffer buffer) {
        partNo = buffer.getInt();
        partsCount = buffer.getInt();
        size = buffer.getInt();
        data = buffer.slice();
    }

    @Override
    protected void writeBody(ByteBuffer buffer) {
        buffer.putInt(partNo);
        buffer.putInt(partsCount);
        buffer.putInt(size);
        buffer.put(data);
    }

    @Override
    public String toString() {
        return "Payload{" +
                "msgHeader=" + getMsgHeader() +
                ", partNo=" + partNo +
                ", partsCount=" + partsCount +
                ", size=" + size +
                '}';
    }
}
