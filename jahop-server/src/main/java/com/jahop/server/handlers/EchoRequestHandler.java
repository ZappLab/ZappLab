package com.jahop.server.handlers;

import com.google.protobuf.TextFormat;
import com.jahop.common.msg.Message;
import com.jahop.common.msg.MessageFactory;
import com.jahop.common.msg.proto.Messages;
import com.jahop.server.connectors.Source;
import com.jahop.server.connectors.Connectors;

public class EchoRequestHandler extends AbstractRequestHandler {
    private long revision = 0;

    public EchoRequestHandler(final Connectors connectors, final MessageFactory messageFactory) {
        super(connectors, messageFactory);
    }

    @Override
    void handleUpdate(Source source, long requestId, Messages.UpdateOrBuilder updateOrBuilder) {
        log.info("Update: {}", TextFormat.shortDebugString(updateOrBuilder));
        revision ++;
        final byte[] data = ((Messages.Update.Builder) updateOrBuilder).build().toByteArray();
        final Message response = messageFactory.createPayload(getRevision(), requestId, data);
        source.send(response);
    }

    @Override
    long getRevision() {
        return revision;
    }
}
