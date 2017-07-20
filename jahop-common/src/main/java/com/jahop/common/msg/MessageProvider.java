package com.jahop.common.msg;

import com.jahop.common.util.Sequencer;

import static com.jahop.common.msg.MessageType.*;

public final class MessageProvider {
    private static final byte VERSION = 1;
    private final int sourceId;
    private final Sequencer sequencer;

    public MessageProvider(final int sourceId, final long seqNum) {
        this.sourceId = sourceId;
        this.sequencer = new Sequencer(seqNum);
    }

    public static Message allocateMessage() {
        return new Message().setPartBytes(new byte[Message.PAYLOAD_MAX_PART_SIZE]);
    }

    public final Message createAck(final long requestId) {
        return next(ACK, Message.ACK_SIZE).setRequestId(requestId);
    }

    public final Message createReject(final long revision, final long requestId, final int error, final String details) {
        return next(REJECT, Message.REJECT_HEADER_SIZE + details.length())
                .setRevision(revision)
                .setRequestId(requestId)
                .setError(error)
                .setDetails(details);
    }

    public final Message createHeartbeat(final long revision) {
        return next(HEARTBEAT, Message.HEARTBEAT_SIZE).setRevision(revision);
    }

    public final Message createPayload(final long revision, final long requestId, final byte[] payload) {
        return next(PAYLOAD, Message.PAYLOAD_HEADER_SIZE + payload.length)
                .setRevision(revision)
                .setRequestId(requestId)
                .setPayloadSize(payload.length)
                .setPartBytes(payload)
                .setPartOffset(0)
                .setPartLength(payload.length)
                .setPartsCount(1)
                .setPartNo(0);
    }

    private Message next(final byte type, final int bodySize) {
        final Message message = new Message();
        message.getHeader()
                .setVersion(VERSION)
                .setType(type)
                .setSourceId(sourceId)
                .setSeqNo(sequencer.next())
                .setTimestampMs(System.currentTimeMillis())
                .setBodySize(bodySize);
        return message;
    }
}
