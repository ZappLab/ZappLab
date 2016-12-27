package com.jahop.common.msg;

import java.nio.ByteBuffer;

/**
 * Base class for all messages. Max message size is limited by 64Kb.
 */
public abstract class Message {
    public static final int MAX_SIZE = 1 << 16;     // 64k
    private final MessageHeader messageHeader = new MessageHeader();

    public MessageHeader getMsgHeader() {
        return messageHeader;
    }

    public final boolean read(final ByteBuffer buffer) {
        if (buffer.remaining() < getSize()) {
            return false;
        }
        messageHeader.read(buffer);
        readBody(buffer);
        return true;
    }

    protected abstract void readBody(final ByteBuffer buffer);

    public final boolean write(final ByteBuffer buffer) {
        if (buffer.capacity() - buffer.position() < getSize()) {
            return false;
        }
        messageHeader.write(buffer);
        writeBody(buffer);
        return true;
    }

    protected abstract void writeBody(final ByteBuffer buffer);

    protected abstract int getSize();
}
