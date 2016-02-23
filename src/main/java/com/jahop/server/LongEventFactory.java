package com.jahop.server;

import com.jahop.server.events.LongEvent;
import com.lmax.disruptor.EventFactory;

public class LongEventFactory implements EventFactory<LongEvent> {
    public LongEvent newInstance() {
        return new LongEvent();
    }
}