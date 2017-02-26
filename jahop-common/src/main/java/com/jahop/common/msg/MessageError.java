package com.jahop.common.msg;

public final class MessageError {
    public static final int UNSET = 0;
    public static final int SYSTEM = 1;
    public static final int SERVER_BUSY = 2;
    public static final int VALIDATION = 3;

    public static final int[] TYPES = new int[]{
            UNSET,
            SYSTEM,
            SERVER_BUSY,
            VALIDATION
    };

    public static final String[] NAMES = new String[]{
            "UNSET",
            "SYSTEM",
            "SERVER_BUSY",
            "VALIDATION"
    };

    public static String toString(final int type) {
        if (type < 0 || type >= NAMES.length) {
            return String.valueOf(type);
        }
        return NAMES[type];
    }
}
