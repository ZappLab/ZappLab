package com.jahop.common.msg;

import java.nio.ByteBuffer;
import java.util.Objects;

import static com.jahop.common.util.ByteUtils.*;

/**
 * Base class for all messages. Max message size is limited by 64Kb.
 *
 * Data message. Contains binary data.
 * If reference data is larger than max partSize, it is uploaded using multiple payload messages.
 * Size: 32 bytes (header) + 24 bytes (payload header) + data.length
 */
public final class Message {
    public static final int MAX_SIZE = 1 << 16;     // 64k
    public static final int REJECT_HEADER_SIZE = 24;
    public static final int PAYLOAD_HEADER_SIZE = 24;
    public static final int PAYLOAD_MAX_PART_SIZE = MAX_SIZE - (MessageHeader.SIZE + PAYLOAD_HEADER_SIZE);

    private final MessageHeader header = new MessageHeader();

    // Payload, Heartbeat
    private long revision;
    // Payload, Ack, Reject
    private long requestId;
    // Reject
    private int error;
    private String details;
    // Payload
    private int payloadSize;
    private int partNo;
    private int partsCount;
    private int partOffset;
    private int partLength;
    private byte[] partBytes;

    public MessageHeader getHeader() {
        return header;
    }

    public long getRevision() {
        return revision;
    }

    public void setRevision(long revision) {
        this.revision = revision;
    }

    public long getRequestId() {
        return requestId;
    }

    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }

    public int getError() {
        return error;
    }

    public void setError(int error) {
        this.error = error;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    public int getPartsCount() {
        return partsCount;
    }

    public void setPartsCount(int partsCount) {
        this.partsCount = partsCount;
    }

    public int getPartNo() {
        return partNo;
    }

    public void setPartNo(int partNo) {
        this.partNo = partNo;
    }

    public int getPayloadSize() {
        return payloadSize;
    }

    public void setPayloadSize(int payloadSize) {
        this.payloadSize = payloadSize;
    }

    public byte[] getPartBytes() {
        return partBytes;
    }

    public void setPartBytes(byte[] partBytes) {
        this.partBytes = partBytes;
    }

    public int getPartOffset() {
        return partOffset;
    }

    public void setPartOffset(int partOffset) {
        this.partOffset = partOffset;
    }

    public int getPartLength() {
        return partLength;
    }

    public void setPartLength(int partLength) {
        this.partLength = partLength;
    }

    public final boolean read(final ByteBuffer buffer) {
        clear();
        return header.read(buffer) && readBody(buffer);
    }

    public boolean readBody(final ByteBuffer buffer) {
        if (buffer.remaining() < header.getBodySize()) {
            return false;
        }
        switch (header.getType()) {
            case MessageType.HEARTBEAT:
                revision = buffer.getLong();
                break;
            case MessageType.ACK:
                requestId = buffer.getLong();
                break;
            case MessageType.REJECT:
                revision = buffer.getLong();
                requestId = buffer.getLong();
                error = buffer.getInt();
                buffer.getInt();        // reserved
                details = readString(buffer, header.getBodySize() - REJECT_HEADER_SIZE);
                break;
            case MessageType.PAYLOAD:
                revision = buffer.getLong();
                requestId = buffer.getLong();
                payloadSize = buffer.getInt();
                partNo = read2LowerBytes(buffer);
                partsCount = read2LowerBytes(buffer);
                readPart(buffer);
                break;
            default:
                throw new RuntimeException("Bad header: " + header);
        }
        return true;
    }

    private void readPart(final ByteBuffer buffer) {
        partOffset = 0;
        partLength = header.getBodySize() - PAYLOAD_HEADER_SIZE;
        if (partBytes == null || partBytes.length < partLength) {
            throw new IllegalStateException("Not enough space allocated to read " + partLength + " bytes");
        }
        buffer.get(partBytes, partOffset, partLength);
    }

    public final boolean write(final ByteBuffer buffer) {
        return buffer.remaining() >= header.getMessageSize() && header.write(buffer) && writeBody(buffer);
    }

    public boolean writeBody(final ByteBuffer buffer) {
        if (buffer.remaining() < header.getBodySize()) {
            return false;
        }
        switch (header.getType()) {
            case MessageType.HEARTBEAT:
                buffer.putLong(revision);
                break;
            case MessageType.ACK:
                buffer.putLong(requestId);
                break;
            case MessageType.REJECT:
                buffer.putLong(revision);
                buffer.putLong(requestId);
                buffer.putInt(error);
                buffer.putInt(0);
                writeString(buffer, details);
                break;
            case MessageType.PAYLOAD:
                buffer.putLong(revision);
                buffer.putLong(requestId);
                buffer.putInt(payloadSize);
                write2LowerBytes(buffer, partNo);
                write2LowerBytes(buffer, partsCount);
                buffer.put(partBytes, partOffset, partLength);
                break;
            default:
                throw new RuntimeException("Bad header: " + header);
        }
        return true;
    }

    public void clear() {
        header.clear();
        revision = 0;
        requestId = 0;
        error = 0;
        details = null;
        payloadSize = 0;
        partNo = 0;
        partsCount = 0;
        partOffset = 0;
        partLength = 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return revision == message.revision &&
                requestId == message.requestId &&
                error == message.error &&
                payloadSize == message.payloadSize &&
                partNo == message.partNo &&
                partsCount == message.partsCount &&
                partOffset == message.partOffset &&
                partLength == message.partLength &&
                Objects.equals(header, message.header) &&
                Objects.equals(details, message.details);
    }

    @Override
    public int hashCode() {
        return Objects.hash(header, revision, requestId, error, details, payloadSize, partNo, partsCount, partOffset, partLength);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder()
                .append("Msg{").append("hdr=").append(header);
        switch (header.getType()) {
            case MessageType.HEARTBEAT:
                sb.append(", rev=").append(revision);
                break;
            case MessageType.ACK:
                sb.append(", reqId=").append(requestId);
                break;
            case MessageType.REJECT:
                sb.append(", rev=").append(revision);
                sb.append(", reqId=").append(requestId);
                sb.append(", err=").append(MessageError.toString(error)).append('#').append(details);
                break;
            case MessageType.PAYLOAD:
                sb.append(", rev=").append(revision);
                sb.append(", reqId=").append(requestId);
                sb.append(", loadSz=").append(payloadSize);
                sb.append(", partNo=").append(partNo);
                sb.append(", partsCnt=").append(partsCount);
                break;
        }
        return sb.append('}').toString();
    }
}
