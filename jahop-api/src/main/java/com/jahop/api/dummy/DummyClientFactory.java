package com.jahop.api.dummy;

import com.jahop.api.Client;
import com.jahop.api.ClientFactory;

public class DummyClientFactory implements ClientFactory {
    @Override
    public Client create(int sourceId) {
        return new DummyClient();
    }
}
