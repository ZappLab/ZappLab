package com.jahop.common.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Bytes manipulation auxiliary methods
 *
 * Created by Pavel on 8/20/2016.
 */
public class ByteUtils {
    public static final int MAX_STRING_LENGTH = 1024;

    public static void write2LowerBytes(final ByteBuffer buffer, final int number) {
        buffer.put((byte) (number >> 8 & 0xff));
        buffer.put((byte) (number & 0xff));
    }

    public static int read2LowerBytes(final ByteBuffer buffer) {
        final int b1 = buffer.get() & 0xff;
        final int b2 = buffer.get() & 0xff;
        return (b1 << 8) | b2;
    }

    // ascii (1 byte) symbols supported
    public static void writeString(final ByteBuffer buffer, final String str) {
        final byte[] data = str.getBytes(StandardCharsets.US_ASCII);
        if (data.length > MAX_STRING_LENGTH) {
            throw new IllegalArgumentException("String is too long: " + data.length);
        }
        buffer.put(data);
    }
    // ascii (1 byte) symbols supported
    public static String readString(final ByteBuffer buffer, final int length) {
        if (length < 0 || length > MAX_STRING_LENGTH) {
            throw new IllegalArgumentException("Bad string length: " + length);
        }
        final byte[] data = new byte[length];
        buffer.get(data);
        return new String(data, StandardCharsets.US_ASCII);
    }
}
