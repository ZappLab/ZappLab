package com.jahop.server;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.jahop.common.msg.*;
import com.jahop.common.msg.proto.Messages;
import com.lmax.disruptor.EventHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.jahop.common.msg.MessageError.VALIDATION;

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
                builder.clear().mergeFrom(message.getPayload(), message.getPartOffset(), message.getPartLength());
                handleUpdate();
            } catch (ValidationException e) {
                log.error("Validation error", e);
                final Message reject = messageFactory.createReject(revision, message.getRequestId(), e.getError(), e.getDetails());
                event.sendResponse(reject);
            } catch (InvalidProtocolBufferException e) {
                log.error("Failed to parse payload", e);
                final Message reject = messageFactory.createReject(revision, message.getRequestId(), MessageError.SYSTEM, "Invalid payload format");
                event.sendResponse(reject);
            }
        } else {
            log.error("Bad request");
        }
    }

    private void validate(final Message message) throws ValidationException {
        final MessageHeader header = message.getHeader();

        if (header.getSourceId() < 1000) {
            throw new ValidationException(VALIDATION, "Bad source id ([0, 999] reserved): " + header.getSourceId());
        }
        if (header.getType() != MessageType.PAYLOAD) {
            throw new ValidationException(VALIDATION, "Bad message type (PAYLOAD expected): " + MessageType.toString(header.getType()));
        }
    }

    private void handleUpdate() {
        log.info("Update: {}", TextFormat.shortDebugString(builder));
    }
}