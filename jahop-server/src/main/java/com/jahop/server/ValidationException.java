package com.jahop.server;

public class ValidationException extends Exception {
    private final int error;
    private final String details;

    public ValidationException(int error, String details) {
        super(details);
        this.error = error;
        this.details = details;
    }

    public int getError() {
        return error;
    }

    public String getDetails() {
        return details;
    }
}
