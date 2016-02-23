package com.jahop.server;

import com.jahop.server.events.LongEvent;
import com.lmax.disruptor.EventHandler;

public class LongEventHandler implements EventHandler<LongEvent> {
    public void onEvent(LongEvent event, long sequence, boolean endOfBatch) {
        System.out.println(System.currentTimeMillis() + " - Event: " + event + ", sequence: " + sequence + ", endOfBatch: " + endOfBatch);
    }
}