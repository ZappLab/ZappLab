package com.jahop.common.msg;

public final class MessageType {
    // Payload messages from server to client
    public static final short HEARTBEAT = 0;
    public static final short ACK = 1;
    public static final short UPDATE = 2;
    public static final short SNAPSHOT = 3;
    // Payload messages from client to server
    public static final short UPDATE_REQUEST = 4;
    public static final short SNAPSHOT_REQUEST = 5;

    public static final String[] NAMES = new String[] {
        "HEARTBEAT",
        "ACK",
        "UPDATE",
        "SNAPSHOT",
        "UPDATE_REQUEST",
        "SNAPSHOT_REQUEST",
    };

    public static String toString(final short type) {
        if (type < 0 || type >= NAMES.length) {
            return String.valueOf(type);
        }
        return NAMES[type];
    }
}
