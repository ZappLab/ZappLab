package com.jahop.server;

import com.jahop.common.msg.Message;
import com.jahop.server.msg.RequestProducer;
import org.springframework.stereotype.Service;

@Service
public interface Connector {
    void start();

    void stop();

    void setProducer(RequestProducer producer);

    void send(final Source source, final Message message);
}