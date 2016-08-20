package com.jahop.server;

import com.jahop.common.msg.Payload;
import com.jahop.common.msg.proto.Messages;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

class RequestProducer {
    private final byte[] data = new byte[Payload.MAX_DATA_SIZE];
    private final Payload payload = new Payload();
    private final Messages.SnapshotRequest.Builder builder = Messages.SnapshotRequest.newBuilder();

    private static final EventTranslatorOneArg<Request, Messages.SnapshotRequest> TRANSLATOR = (event, sequence, request) -> {
        final String name = request.getPath(0);
        event.setName(name);
    };

    private final RingBuffer<Request> ringBuffer;

    RequestProducer(final RingBuffer<Request> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    void onData(final ByteBuffer buffer) throws IOException {
        payload.read(buffer);
        final int dataLength = payload.getSize();
        System.out.println(payload);
        buffer.get(data, 0, dataLength);
        final Messages.SnapshotRequest request = builder.clear().mergeFrom(data, 0, dataLength).build();
        ringBuffer.publishEvent(TRANSLATOR, request);
    }
}
