package com.jahop.server;

import com.lmax.disruptor.EventHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RequestHandler implements EventHandler<Request> {
    private static final Logger log = LogManager.getLogger(RequestHandler.class);
    public void onEvent(Request event, long sequence, boolean endOfBatch) {
        log.info("Event: {}, sequence: {}, endOfBatch: {}", event, sequence, endOfBatch);
    }
}