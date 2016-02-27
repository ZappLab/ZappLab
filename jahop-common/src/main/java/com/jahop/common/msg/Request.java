package com.jahop.common.msg;

public class Request {
    private long seqNo;
    private long sourceId;

    public long getSeqNo() {
        return seqNo;
    }

    public void setSeqNo(long seqNo) {
        this.seqNo = seqNo;
    }

    public long getSourceId() {
        return sourceId;
    }

    public void setSourceId(long sourceId) {
        this.sourceId = sourceId;
    }

    @Override
    public String toString() {
        return "ServerEvent{" +
                "seqNo=" + seqNo +
                ", sourceId=" + sourceId +
                '}';
    }
}