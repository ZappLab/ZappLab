package com.zapplab.server;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.zapplab.server.events.LongEvent;
import com.lmax.disruptor.RingBuffer;

import java.nio.ByteBuffer;

public class LongEventProducer {
    private static final EventTranslatorOneArg<LongEvent, ByteBuffer> TRANSLATOR = (event, sequence, bb) -> event.set(bb.getLong(0));
    private final RingBuffer<LongEvent> ringBuffer;

    public LongEventProducer(RingBuffer<LongEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void onData(ByteBuffer bb) {
        ringBuffer.publishEvent(TRANSLATOR, bb);
    }
}
