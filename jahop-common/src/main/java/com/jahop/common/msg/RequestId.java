package com.jahop.common.msg;

public final class RequestId {
    private final int sourceId;
    private final long requestId;

    public RequestId(int sourceId, long requestId) {
        this.sourceId = sourceId;
        this.requestId = requestId;
    }

    public int getSourceId() {
        return sourceId;
    }

    public long getRequestId() {
        return requestId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RequestId requestId = (RequestId) o;

        return this.requestId == requestId.requestId && sourceId == requestId.sourceId;

    }

    @Override
    public int hashCode() {
        int result = sourceId;
        result = 31 * result + (int) (requestId ^ (requestId >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return sourceId + "#" + requestId;
    }
}
