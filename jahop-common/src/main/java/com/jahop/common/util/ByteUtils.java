package com.jahop.common.util;

import java.nio.ByteBuffer;

/**
 * Bytes manipulation auxiliary methods
 *
 * Created by Pavel on 8/20/2016.
 */
public class ByteUtils {
    public static void write2LowerBytes(final ByteBuffer buffer, final int number) {
        buffer.put((byte) (number >> 8 & 0xff));
        buffer.put((byte) (number & 0xff));
    }

    public static int read2LowerBytes(final ByteBuffer buffer) {
        final int b1 = buffer.get() & 0xff;
        final int b2 = buffer.get() & 0xff;
        return (b1 << 8) | b2;
    }
}
