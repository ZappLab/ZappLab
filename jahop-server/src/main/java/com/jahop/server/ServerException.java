package com.jahop.server;

public class ServerException extends RuntimeException {
    private final int error;

    public ServerException(int error, String message) {
        super(message);
        this.error = error;
    }

    public ServerException(int error, String message, Throwable cause) {
        super(message, cause);
        this.error = error;
    }

    public int getError() {
        return error;
    }
}
