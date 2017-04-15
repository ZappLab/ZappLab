package com.jahop.api.utils;

import com.jahop.api.Environment;
import com.jahop.api.Transport;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public final class Resources {
    private static final String RESOURCES_DIR = "com/jahop/api/resources/";

    public static Properties load(Transport transport, Environment environment) {
        final Properties properties = new Properties();
        final String transportName = transport.toString().toLowerCase();
        final String envName = environment.toString().toLowerCase();
        load(properties, "api.properties");
        load(properties, String.format("%s/transport.properties", transportName));
        load(properties, String.format("%s/%s/env.properties", transportName, envName));
        return properties;
    }

    public static void load(final Properties properties, final String resource) {
        final URL url = Resources.class.getClassLoader().getResource(RESOURCES_DIR + resource);
        if (url != null) {
            try (InputStream in = url.openStream()) {
                properties.load(in);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load " + url.getPath(), e);
            }
        }
    }

    public static String getOrThrow(final Properties properties, final String name) {
        final String value = properties.getProperty(name);
        if (value == null) {
            throw new RuntimeException(String.format("Required property '%s' is missing", name));
        }
        return value;
    }
}
