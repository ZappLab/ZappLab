package com.jahop.server.connectors;

import com.jahop.common.msg.Message;
import org.springframework.stereotype.Service;

@Service
public interface Connector {
    void start();

    void stop();

    void send(final Source source, final Message message);

    void send(final Message message);
}