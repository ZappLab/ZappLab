package com.jahop.server;

import com.jahop.common.msg.MessageFactory;
import com.jahop.common.msg.MessageType;
import com.jahop.common.msg.Payload;
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

    public void onEvent(Request event, long sequence, boolean endOfBatch) {
        log.info("Request: {}, sequence: {}, endOfBatch: {}", event, sequence, endOfBatch);
        if (event.isSnapshotRequest()) {
            handleSnapshot(event);
        } else {
            handleUpdate(event);
        }
    }

    private void handleUpdate(Request event) {
        final Payload payload = messageFactory.createPayload(MessageType.UPDATE);
        event.sendResponse(payload);
    }

    private void handleSnapshot(Request event) {
        final Payload payload = messageFactory.createPayload(MessageType.SNAPSHOT);
        event.sendResponse(payload);
    }
}