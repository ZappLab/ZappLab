package com.jahop.server;

import com.jahop.common.msg.Request;
import com.lmax.disruptor.EventHandler;

public class ServerEventHandler implements EventHandler<Request> {
    public void onEvent(Request event, long sequence, boolean endOfBatch) {
        System.out.println(System.currentTimeMillis() + " - Event: " + event + ", sequence: " + sequence + ", endOfBatch: " + endOfBatch);
    }
}