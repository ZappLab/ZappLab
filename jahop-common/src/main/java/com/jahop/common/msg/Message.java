package com.jahop.common.msg;

import java.nio.ByteBuffer;

import static com.jahop.common.util.ByteUtils.read2LowerBytes;
import static com.jahop.common.util.ByteUtils.write2LowerBytes;

/**
 * Base class for all messages. Max message size is limited by 64Kb.
 *
 * Data message. Contains binary data.
 * If reference data is larger than max partSize, it is uploaded using multiple payload messages.
 * Size: 32 bytes (header) + 24 bytes (payload header) + data.length
 */
public final class Message {
    public static final int MAX_SIZE = 1 << 16;     // 64k
    public static final int PAYLOAD_HEADER_SIZE = 24;
    public static final int PAYLOAD_MAX_PART_SIZE = MAX_SIZE - (MessageHeader.SIZE + PAYLOAD_HEADER_SIZE);

    private final MessageHeader header = new MessageHeader();

    // Payload, Heartbeat
    private long revision;
    // Payload, Ack
    private long requestId;
    // Payload
    private int payloadSize;
    private int partNo;
    private int partsCount;
    private int partOffset;
    private int partLength;
    private byte[] payload;

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

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
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
        if (buffer.remaining() < MessageHeader.SIZE) {
            return false;
        }
        header.read(buffer);
        if (buffer.remaining() < header.getBodySize()) {
            return false;
        }
        readBody(buffer);
        return true;
    }

    private void readBody(final ByteBuffer buffer) {
        switch (header.getType()) {
            case MessageType.HEARTBEAT:
                revision = buffer.getLong();
                break;
            case MessageType.ACK:
                requestId = buffer.getLong();
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
    }

    private void readPart(final ByteBuffer buffer) {
        partOffset = 0;
        partLength = header.getBodySize() - PAYLOAD_HEADER_SIZE;
        if (payload == null || payload.length < partLength) {
            throw new IllegalStateException("Not enough space allocated to read " + partLength + " bytes");
        }
        buffer.get(payload, partOffset, partLength);
    }

    public final boolean write(final ByteBuffer buffer) {
        if (buffer.capacity() - buffer.position() < MessageHeader.SIZE + header.getBodySize()) {
            return false;
        }
        header.write(buffer);
        writeBody(buffer);
        return true;
    }

    private void writeBody(final ByteBuffer buffer) {
        switch (header.getType()) {
            case MessageType.HEARTBEAT:
                buffer.putLong(revision);
                break;
            case MessageType.ACK:
                buffer.putLong(requestId);
                break;
            case MessageType.PAYLOAD:
                buffer.putLong(revision);
                buffer.putLong(requestId);
                buffer.putInt(payloadSize);
                write2LowerBytes(buffer, partNo);
                write2LowerBytes(buffer, partsCount);
                buffer.put(payload, partOffset, partLength);
                break;
            default:
                throw new RuntimeException("Bad header: " + header);
        }
    }

    public void clear() {
        header.clear();
        revision = 0;
        requestId = 0;
        payloadSize = 0;
        partNo = 0;
        partsCount = 0;
        partOffset = 0;
        partLength = 0;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder()
                .append("Message{").append("header=").append(header);
        switch (header.getType()) {
            case MessageType.HEARTBEAT:
                sb.append(", revision=").append(revision);
                break;
            case MessageType.ACK:
                sb.append(", requestId=").append(requestId);
                break;
            case MessageType.PAYLOAD:
                sb.append(", revision=").append(revision);
                sb.append(", requestId=").append(requestId);
                sb.append(", payloadSize=").append(payloadSize);
                sb.append(", partNo=").append(partNo);
                sb.append(", partsCount=").append(partsCount);
                break;
        }
        return sb.append('}').toString();
    }
}
