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
        if (event.isReady()) {
            final Message message = event.getMessage();
            log.info("onEvent: sequence={}, message={}", sequence, message);

            try {
                validate(message);
                builder.clear().mergeFrom(message.getPartBytes(), message.getPartOffset(), message.getPartLength());
                handleUpdate();
            } catch (ServerException e) {
                log.error("Server error", e);
                final Message reject = messageFactory.createReject(revision, message.getRequestId(), e.getError(), e.getMessage());
                event.getSource().send(reject);
            } catch (InvalidProtocolBufferException e) {
                log.error("Failed to parse payload", e);
                final Message reject = messageFactory.createReject(revision, message.getRequestId(), Errors.REQUEST_BROKEN_MESSAGE, "Invalid payload format");
                event.getSource().send(reject);
            }
        } else {
            log.error("Bad request");
        }
    }

    private void validate(final Message message) throws ServerException {
        final MessageHeader header = message.getHeader();

        if (header.getSourceId() < 1000) {
            throw new ServerException(Errors.REQUEST_BAD_SOURCE_ID, "Bad source id ([0, 999] reserved): " + header.getSourceId());
        }
        if (header.getType() != MessageType.PAYLOAD) {
            throw new ServerException(Errors.REQUEST_BAD_MESSAGE_TYPE, "Bad message type (PAYLOAD expected): " + MessageType.toString(header.getType()));
        }
    }

    private void handleUpdate() {
        log.info("Update: {}", TextFormat.shortDebugString(builder));
    }
}