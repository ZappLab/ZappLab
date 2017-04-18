package com.jahop.server.handlers;

import com.google.protobuf.InvalidProtocolBufferException;
import com.jahop.common.msg.*;
import com.jahop.common.msg.proto.Messages;
import com.jahop.server.Errors;
import com.jahop.server.ServerException;
import com.jahop.server.connectors.Source;
import com.jahop.server.connectors.Connectors;
import com.jahop.server.msg.Request;
import com.lmax.disruptor.EventHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractRequestHandler implements EventHandler<Request> {
    private static final Messages.Update.Builder builder = Messages.Update.newBuilder();
    protected final Logger log = LogManager.getLogger(AbstractRequestHandler.class);
    protected final Connectors connectors;
    protected final MessageFactory messageFactory;

    public AbstractRequestHandler(Connectors connectors, MessageFactory messageFactory) {
        this.connectors = connectors;
        this.messageFactory = messageFactory;
    }

    public final void onEvent(final Request event, final long sequence, final boolean endOfBatch) {
        final Message message = event.getMessage();
        if (event.isValid()) {
            if (log.isDebugEnabled()) {
                log.debug("Incoming: " + message);
            }

            final Source source = event.getSource();
            final long requestId = message.getRequestId();
            try {
                validate(message);
                builder.clear().mergeFrom(message.getPartBytes(), message.getPartOffset(), message.getPartLength());
                handleUpdate(source, requestId, builder);
            } catch (ServerException e) {
                log.info("Message validation failed: " + e.getMessage());
                handleReject(source, requestId, e.getError(), e.getMessage());
            } catch (InvalidProtocolBufferException e) {
                log.error("Invalid message format", e);
                handleReject(source, requestId, Errors.MESSAGE_INVALID_FORMAT, "Invalid message format: " + e.getMessage());
            } catch (Exception e) {
                log.error("Event handler error", e);
                handleReject(source, requestId, Errors.SYSTEM_EVENT_HANDLER, "Event handler error: " + e.getMessage());
            }
        } else {
            log.error("Invalid request message: " + message.getHeader());
        }
    }

    protected void validate(final Message message) {
        final MessageHeader header = message.getHeader();

        if (header.getSourceId() < 1000) {
            throw new ServerException(Errors.MESSAGE_BAD_SOURCE_ID, "Bad source id ([0, 999] reserved): " + header.getSourceId());
        }
        if (header.getType() != MessageType.PAYLOAD) {
            throw new ServerException(Errors.MESSAGE_BAD_TYPE, "Bad message type (PAYLOAD expected): " + MessageType.toString(header.getType()));
        }
    }

    protected void handleReject(final Source source, final long requestId, final int error, final String message) {
        final Message reject = messageFactory.createReject(getRevision(), requestId, error, message);
        source.send(reject);
    }

    abstract void handleUpdate(final Source source, final long requestId, final Messages.UpdateOrBuilder updateOrBuilder);

    abstract long getRevision();
}