package com.jahop.common.msg;

public final class MessageType {
    public static final byte UNKNOWN = 0;
    public static final byte HEARTBEAT = 1;
    public static final byte ACK = 2;
    public static final byte PAYLOAD = 3;

    public static final byte[] TYPES = new byte[]{
            UNKNOWN,
            HEARTBEAT,
            ACK,
            PAYLOAD
    };

    public static final String[] NAMES = new String[]{
            "UNKNOWN",
            "HEARTBEAT",
            "ACK",
            "PAYLOAD"
    };

    public static String toString(final byte type) {
        if (type < 0 || type >= NAMES.length) {
            return String.valueOf(type);
        }
        return NAMES[type];
    }
}
