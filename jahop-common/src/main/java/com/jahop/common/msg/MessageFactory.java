package com.jahop.common.msg;

import com.jahop.common.util.Sequencer;

public final class MessageFactory {
    private static final short VERSION = 1;
    private final int sourceId;
    private final Sequencer sequencer;

    public MessageFactory(int sourceId) {
        this.sourceId = sourceId;
        this.sequencer = new Sequencer();
    }

    public final Ack createAck(final long requestId) {
        final Ack ack = new Ack();
        fillHeader(ack, MessageType.ACK);
        ack.setRequestId(requestId);
        return ack;
    }

    public final Payload createPayload(final short type) {
        final Payload payload = new Payload();
        fillHeader(payload, type);
        return payload;
    }

    public final <T extends Message> T fillHeader(final T message, final short type) {
        final MessageHeader header = message.getHeader();
        header.setType(type);
        header.setVersion(VERSION);
        header.setSourceId(sourceId);
        header.setSeqNo(sequencer.next());
        header.setTimestampMs(System.currentTimeMillis());
        return message;
    }
}
