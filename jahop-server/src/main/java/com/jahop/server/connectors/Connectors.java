package com.jahop.server.connectors;

import com.google.common.collect.Lists;
import com.jahop.common.msg.Message;

import java.util.List;

public final class Connectors implements Connector {
    private final List<Connector> connectors;

    public Connectors(Connector... connectors) {
        this.connectors = Lists.newArrayList(connectors);
    }

    @Override
    public void start() {
        connectors.forEach(Connector::start);
    }

    @Override
    public void stop() {
        connectors.forEach(Connector::stop);
    }

    @Override
    public void send(final Source source, final Message message) {
        connectors.forEach(connector -> connector.send(source, message));
    }

    @Override
    public void send(Message message) {
        connectors.forEach(connector -> connector.send(message));
    }
}
