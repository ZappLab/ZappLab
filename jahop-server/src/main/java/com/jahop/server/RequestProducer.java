package com.jahop.server;

import com.jahop.common.msg.Payload;
import com.jahop.common.msg.proto.Messages;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

class RequestProducer {
    private static final Logger log = LogManager.getLogger(RequestProducer.class);
    private final byte[] data = new byte[Payload.MAX_PART_SIZE];
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
        while (payload.read(buffer)) {
            final int partSize = payload.getPartSize();
            log.info("Received: {}", payload);
            if (buffer.remaining() < partSize) {
                log.error("Broken data. Expected: {} bytes, actual: {} bytes", partSize, buffer.remaining());
            } else {
                buffer.get(data, 0, partSize);
                final Messages.SnapshotRequest request = builder.clear().mergeFrom(data, 0, partSize).build();
                ringBuffer.publishEvent(TRANSLATOR, request);
            }
        }
    }
}
