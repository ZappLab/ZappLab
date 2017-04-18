package com.jahop.server.msg;

import com.jahop.common.msg.Message;
import com.jahop.server.connectors.Source;

public final class Request {
    private final Message message;
    private Source source;
    private boolean valid;

    public Request(Message message) {
        this.message = message;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public Message getMessage() {
        return message;
    }
}