package com.jahop.api.tcp;

import com.jahop.api.Client;
import com.jahop.api.ClientFactory;

import java.util.Properties;

/**
 * Created by Pavel on 9/1/2016.
 */
public class TcpClientFactory implements ClientFactory {
    private static final String PARAM_SERVER_HOST = "jahop.server.host";
    private static final String PARAM_SERVER_PORT = "jahop.server.port";
    @Override
    public Client create(Properties props) {
        final String host = props.getProperty(PARAM_SERVER_HOST);
        int port;
        try {
            port = Integer.parseInt(props.getProperty(PARAM_SERVER_PORT));
        } catch (Exception e) {
            port = 9090;
        }
        return new TcpClient(host, port);
    }
}
