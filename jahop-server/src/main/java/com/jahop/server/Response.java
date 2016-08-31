package com.jahop.server;

public class Response {
    public static final int CODE_SUCCESS = 0;
    public static final int CODE_ERROR = 1;

    private int code;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    @Override
    public String toString() {
        return "Response{" +
                "code=" + code +
                '}';
    }
}