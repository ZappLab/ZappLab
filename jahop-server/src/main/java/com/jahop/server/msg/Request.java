package com.jahop.server.msg;

import com.jahop.common.msg.Message;
import com.jahop.common.msg.MessageHeader;
import com.jahop.server.Source;

import java.nio.ByteBuffer;

public final class Request {
    enum Status {OK, ERROR}
    private final Message message;
    private Source source;
    private Status status;

    public Request(Message message) {
        this.message = message;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public Message getMessage() {
        return message;
    }

    public void read(final MessageHeader header, final ByteBuffer buffer) {
        message.getHeader().copyFrom(header);
        status = message.readBody(buffer) ? Status.OK : Status.ERROR;
    }

    public boolean isValid() {
        return status == Status.OK;
    }
}