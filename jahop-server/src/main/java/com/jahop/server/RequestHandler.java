package com.jahop.server;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.jahop.common.msg.*;
import com.jahop.common.msg.proto.Messages;
import com.jahop.server.msg.Request;
import com.lmax.disruptor.EventHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RequestHandler implements EventHandler<Request> {
    private static final Logger log = LogManager.getLogger(RequestHandler.class);
    private static final Messages.Update.Builder builder = Messages.Update.newBuilder();
    private final MessageFactory messageFactory;
    private long revision;

    public RequestHandler(MessageFactory messageFactory) {
        this.messageFactory = messageFactory;
    }

    public void onEvent(final Request event, final long sequence, final boolean endOfBatch) {
        if (event.isValid()) {
            final Message message = event.getMessage();
            if (log.isDebugEnabled()) {
                log.debug("Incoming: " + message);
            }

            try {
                validate(message);
                builder.clear().mergeFrom(message.getPartBytes(), message.getPartOffset(), message.getPartLength());
                handleUpdate();
            } catch (ServerException e) {
                log.info("Message validation failed: " + e.getMessage());
                sendReject(event, e.getError(), e.getMessage());
            } catch (InvalidProtocolBufferException e) {
                log.error("Invalid message format", e);
                sendReject(event, Errors.MESSAGE_INVALID_FORMAT, "Invalid message format: " + e.getMessage());
            } catch (Exception e) {
                log.error("Event handler error", e);
                sendReject(event, Errors.SYSTEM_EVENT_HANDLER, "Event handler error: " + e.getMessage());
            }
        } else {
            log.error("Invalid request");
        }
    }

    private void validate(final Message message) {
        final MessageHeader header = message.getHeader();

        if (header.getSourceId() < 1000) {
            throw new ServerException(Errors.MESSAGE_BAD_SOURCE_ID, "Bad source id ([0, 999] reserved): " + header.getSourceId());
        }
        if (header.getType() != MessageType.PAYLOAD) {
            throw new ServerException(Errors.MESSAGE_BAD_TYPE, "Bad message type (PAYLOAD expected): " + MessageType.toString(header.getType()));
        }
    }

    private void sendReject(final Request event, int error, String message) {
        final Message reject = messageFactory.createReject(revision, event.getMessage().getRequestId(), error, message);
        event.getSource().send(reject);
    }

    private void handleUpdate() {
        log.info("Update: {}", TextFormat.shortDebugString(builder));
    }
}