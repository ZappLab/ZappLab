package com.jahop.server;

import com.jahop.common.msg.Payload;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;

public class RequestProducer {
    private static final EventTranslatorOneArg<Request, Payload> TRANSLATOR = (event, sequence, payload) -> {
        event.setData(payload.getData().get());
    };

    private final RingBuffer<Request> ringBuffer;

    public RequestProducer(final RingBuffer<Request> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void onData(final Payload payload) {
        ringBuffer.publishEvent(TRANSLATOR, payload);
    }
}
