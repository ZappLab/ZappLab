package com.jahop.server;

import com.jahop.common.msg.Request;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;

import java.nio.ByteBuffer;

public class RequestProducer {
    private static final EventTranslatorOneArg<Request, ByteBuffer> TRANSLATOR = (event, sequence, bb) -> {
        event.setSeqNo(bb.getLong(0));
        event.setSourceId(bb.getLong(8));
    };

    private final RingBuffer<Request> ringBuffer;

    public RequestProducer(final RingBuffer<Request> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void onData(ByteBuffer bb) {
        ringBuffer.publishEvent(TRANSLATOR, bb);
    }
}
