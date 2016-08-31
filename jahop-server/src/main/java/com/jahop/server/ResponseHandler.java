package com.jahop.server;

import com.lmax.disruptor.EventHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResponseHandler implements EventHandler<Response> {
    private static final Logger log = LogManager.getLogger(ResponseHandler.class);
    public void onEvent(Response event, long sequence, boolean endOfBatch) {
        log.info("Event: {}, sequence: {}, endOfBatch: {}", event, sequence, endOfBatch);
    }
}