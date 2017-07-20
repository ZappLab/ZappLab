package com.jahop.api.dummy;

import com.google.protobuf.GeneratedMessage;
import com.jahop.api.Client;

public class DummyClient implements Client {
    @Override
    public void send(GeneratedMessage message) {
    }

    @Override
    public int getSourceId() {
        return 0;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
}
