package com.jahop.api.tcp;

import com.jahop.api.Client;
import com.jahop.api.ClientFactory;
import com.jahop.api.utils.Resources;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Properties;

public class TcpClientFactory implements ClientFactory {
    private static final String PARAM_SERVER_ADDRESS = "jahop.server.address";

    private volatile SocketAddress serverAddress;

    @Override
    public void init(Properties properties) {
        final String addressString = Resources.getOrThrow(properties, PARAM_SERVER_ADDRESS);
        final URI address = URI.create(addressString);
        serverAddress = new InetSocketAddress(address.getHost(), address.getPort());
    }

    @Override
    public Client create(int sourceId) {
        if (serverAddress == null) {
            throw new RuntimeException("Factory is not initialized");
        }
        return new TcpClient(serverAddress, sourceId);
    }
}
