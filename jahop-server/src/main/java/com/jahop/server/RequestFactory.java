package com.jahop.server;

import com.jahop.common.msg.MessageFactory;
import com.lmax.disruptor.EventFactory;

public class RequestFactory implements EventFactory<Request> {
    public Request newInstance() {
        return new Request(MessageFactory.allocateMessage());
    }
}