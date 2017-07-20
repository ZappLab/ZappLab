package com.jahop.api;

import com.google.protobuf.GeneratedMessage;

public interface Client {
    void send(GeneratedMessage message);
    int getSourceId();
    void start();
    void stop();
}
