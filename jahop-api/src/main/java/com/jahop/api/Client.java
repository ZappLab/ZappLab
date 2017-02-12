package com.jahop.api;

import java.io.IOException;

/**
 * Created by Pavel on 9/1/2016.
 */
public interface Client {
    Sender getSender(String topic);
    void connect() throws IOException;
    void close() throws IOException;
}
