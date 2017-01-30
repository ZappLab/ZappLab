package com.jahop.server;

import com.jahop.common.msg.MessageFactory;
import com.lmax.disruptor.EventFactory;

public class RequestFactory implements EventFactory<Request> {
    public Request newInstance() {
        final Request request = new Request();
        request.setMessage(MessageFactory.allocateMessage());
        return request;
    }
}