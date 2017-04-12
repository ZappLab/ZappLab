package com.jahop.server.msg;

import com.jahop.common.msg.MessageHeader;
import com.jahop.server.Source;
import com.lmax.disruptor.RingBuffer;

import java.nio.ByteBuffer;

public class RequestProducer {
    private final RingBuffer<Request> ringBuffer;

    public RequestProducer(final RingBuffer<Request> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void onData(final Source source, final MessageHeader header, final ByteBuffer buffer) {
        final long sequence = ringBuffer.next();
        try {
            final Request request = ringBuffer.get(sequence);
            request.setSource(source);
            request.read(header, buffer);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}
