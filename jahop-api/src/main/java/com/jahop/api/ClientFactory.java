package com.jahop.api;

import java.util.Properties;

import static com.jahop.api.utils.Resources.getOrThrow;
import static com.jahop.api.utils.Resources.load;

public interface ClientFactory {
    String PARAM_CLIENT_FACTORY_CLASS = "jahop.client.factory.class";

    static ClientFactory newInstance(Transport transport, Environment environment) {
        final Properties properties = load(transport, environment);
        final String clientFactoryClass = getOrThrow(properties, PARAM_CLIENT_FACTORY_CLASS);

        try {
            final ClientFactory factory = (ClientFactory) Class.forName(clientFactoryClass).newInstance();
            factory.init(properties);
            return factory;
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to init client factory", e);
        }
    }

    Client create(int sourceId);

    default void init(Properties properties) {
    }
}
