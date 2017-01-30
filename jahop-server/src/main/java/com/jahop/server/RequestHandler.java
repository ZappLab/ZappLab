package com.jahop.server;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.jahop.common.msg.Message;
import com.jahop.common.msg.MessageFactory;
import com.jahop.common.msg.MessageType;
import com.jahop.common.msg.proto.Messages;
import com.lmax.disruptor.EventHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RequestHandler implements EventHandler<Request> {
    private static final Logger log = LogManager.getLogger(RequestHandler.class);
    private static final Messages.Update.Builder builder = Messages.Update.newBuilder();
    private final MessageFactory messageFactory;

    public RequestHandler(MessageFactory messageFactory) {
        this.messageFactory = messageFactory;
    }

    public void onEvent(final Request event, final long sequence, final boolean endOfBatch) {
        final Message message = event.getMessage();
        log.info("onEvent: sequence={}, message={}", sequence, message);
        final byte type = message.getHeader().getType();
        if (type == MessageType.PAYLOAD) {
            try {
                builder.mergeFrom(message.getPayload(), message.getPartOffset(), message.getPartLength());
            } catch (InvalidProtocolBufferException e) {
                log.error("Failed to parse payload", e);
            }
            handleUpdate();
        } else {
            log.error("Unexpected message type: " + message);
        }
    }

    private void handleUpdate() {
        log.info("Update: {}", TextFormat.shortDebugString(builder));
    }
}