package com.jahop.api;

import com.jahop.common.msg.Message;
import com.lmax.disruptor.EventHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResponseHandler implements EventHandler<Message> {
    private static final Logger log = LogManager.getLogger(ResponseHandler.class);
    @Override
    public void onEvent(Message event, long sequence, boolean endOfBatch) throws Exception {
        log.info("onEvent: {}", event);
    }
}
