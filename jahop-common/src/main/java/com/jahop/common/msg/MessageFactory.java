package com.jahop.common.msg;

import com.jahop.common.util.Sequencer;

import static com.jahop.common.msg.MessageType.*;

public final class MessageFactory {
    private final byte VERSION = 1;
    private final int sourceId;
    private final Sequencer sequencer;

    public MessageFactory(final int sourceId, final Sequencer sequencer) {
        this.sourceId = sourceId;
        this.sequencer = sequencer;
    }

    public static Message allocateMessage() {
        final Message message = new Message();
        message.setPayload(new byte[Message.PAYLOAD_MAX_PART_SIZE]);
        return message;
    }

    public final Message createAck(final long requestId) {
        final Message message = new Message();
        populateHeader(message, ACK, 8);
        message.setRequestId(requestId);
        return message;
    }

    public final Message createReject(final long revision, final long requestId, final int error, final String details) {
        final Message message = new Message();
        populateHeader(message, REJECT, Message.REJECT_HEADER_SIZE + details.length());
        message.setRevision(revision);
        message.setRequestId(requestId);
        message.setError(error);
        message.setDetails(details);
        return message;
    }

    public final Message createHeartbeat(final long revision) {
        final Message message = new Message();
        populateHeader(message, HEARTBEAT, 8);
        message.setRevision(revision);
        return message;
    }

    public final Message createPayload(final long revision, final long requestId, final byte[] payload) {
        final Message message = new Message();
        populateHeader(message, PAYLOAD, Message.PAYLOAD_HEADER_SIZE + payload.length);
        message.setRevision(revision);
        message.setRequestId(requestId);
        message.setPayloadSize(payload.length);
        message.setPayload(payload);
        message.setPartOffset(0);
        message.setPartLength(payload.length);
        message.setPartsCount(1);
        message.setPartNo(0);
        return message;
    }

    private Message populateHeader(final Message message, final byte type, final int bodySize) {
        final MessageHeader header = message.getHeader();
        header.setVersion(VERSION);
        header.setType(type);
        header.setSourceId(sourceId);
        header.setSeqNo(sequencer.next());
        header.setTimestampMs(System.currentTimeMillis());
        header.setBodySize(bodySize);
        return message;
    }
}
