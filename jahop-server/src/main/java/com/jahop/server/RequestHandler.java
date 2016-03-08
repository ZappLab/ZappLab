package com.jahop.server;

import com.lmax.disruptor.EventHandler;

public class RequestHandler implements EventHandler<Request> {
    public void onEvent(Request event, long sequence, boolean endOfBatch) {
        System.out.println(System.currentTimeMillis() + " - Event: " + event + ", sequence: " + sequence + ", endOfBatch: " + endOfBatch);
    }
}