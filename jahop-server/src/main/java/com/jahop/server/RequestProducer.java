package com.jahop.server;

import com.jahop.common.msg.MessageFactory;
import com.jahop.common.msg.Payload;
import com.jahop.common.msg.proto.Messages;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.RingBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static com.jahop.common.msg.MessageType.SNAPSHOT_REQUEST;
import static com.jahop.common.msg.MessageType.UPDATE_REQUEST;

class RequestProducer {
    private static final Logger log = LogManager.getLogger(RequestProducer.class);
    private static final EventTranslatorThreeArg<Request, ServerLoop, SocketChannel, Messages.SnapshotRequest> SNAPSHOT_TRANSLATOR =
            (event, sequence, serverLoop, socketChannel, message) -> {
                event.setServerLoop(serverLoop);
                event.setSocketChannel(socketChannel);
                event.setSnapshotRequest(message);
            };
    private static final EventTranslatorThreeArg<Request, ServerLoop, SocketChannel, Messages.UpdateRequest> UPDATE_TRANSLATOR =
            (event, sequence, serverLoop, socketChannel, message) -> {
                event.setServerLoop(serverLoop);
                event.setSocketChannel(socketChannel);
                event.setUpdateRequest(message);
            };

    private final Payload payload = new Payload();
    private final byte[] payloadData = new byte[Payload.MAX_PART_SIZE];
    private final Messages.SnapshotRequest.Builder snapshotRequestBuilder = Messages.SnapshotRequest.newBuilder();
    private final Messages.UpdateRequest.Builder updateRequestBuilder = Messages.UpdateRequest.newBuilder();
    private final RingBuffer<Request> requestBuffer;
    private final MessageFactory messageFactory;

    RequestProducer(final RingBuffer<Request> requestBuffer, MessageFactory messageFactory) {
        this.requestBuffer = requestBuffer;
        this.messageFactory = messageFactory;
    }

    void onData(ServerLoop serverLoop, SocketChannel socketChannel, final ByteBuffer buffer) throws IOException {
        while (payload.read(buffer)) {
            final short type = payload.getHeader().getType();
            final long requestId = payload.getRequestId();
            final int partSize = payload.getPartSize();
            log.info("{}: received {}", socketChannel.getRemoteAddress(), payload);
            if (buffer.remaining() < partSize) {
                log.error("{}: expected {} bytes, actual {} bytes", socketChannel.getRemoteAddress(), partSize, buffer.remaining());
                break;
            }
            buffer.get(payloadData, 0, partSize);
            if (type == SNAPSHOT_REQUEST) {
                final Messages.SnapshotRequest proto = snapshotRequestBuilder.clear().mergeFrom(payloadData, 0, partSize).build();
                requestBuffer.publishEvent(SNAPSHOT_TRANSLATOR, serverLoop, socketChannel, proto);
            } else if (type == UPDATE_REQUEST) {
                final Messages.UpdateRequest proto = updateRequestBuilder.clear().mergeFrom(payloadData, 0, partSize).build();
                requestBuffer.publishEvent(UPDATE_TRANSLATOR, serverLoop, socketChannel, proto);
            } else {
                throw new RuntimeException("Unknown type");
            }
            serverLoop.send(socketChannel, messageFactory.createAck(requestId));
        }
    }
}
