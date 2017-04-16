package com.jahop.console;

import com.jahop.api.Environment;
import com.jahop.api.Transport;

public class Context {
    private final Transport transport;
    private final Environment environment;
    private final boolean debug;

    public Context(Transport transport, Environment environment, boolean debug) {
        this.transport = transport;
        this.environment = environment;
        this.debug = debug;
    }

    public Transport getTransport() {
        return transport;
    }

    public Environment getEnvironment() {
        return environment;
    }

    public boolean isDebug() {
        return debug;
    }
}
