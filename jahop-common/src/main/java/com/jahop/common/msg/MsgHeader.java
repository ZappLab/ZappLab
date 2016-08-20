package com.jahop.common.msg;

public final class MsgHeader {
    public static final int SIZE = 24;  //Header size is 24 bytes
    private short msgType;              //  pos=0, size=2
    private short msgVersion;           //  pos=2, size=2
    private int sourceId;               //  pos=4, size=4
    private long seqNo;                 //  pos=8, size=8
    private long timestampMs;           //  pos=16, size=8

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

    @Override
    public String toString() {
        return "MsgHeader{" +
                "msgType=" + msgType +
                ", msgVersion=" + msgVersion +
                ", sourceId=" + sourceId +
                ", seqNo=" + seqNo +
                ", timestampMs=" + timestampMs +
                '}';
    }
}
