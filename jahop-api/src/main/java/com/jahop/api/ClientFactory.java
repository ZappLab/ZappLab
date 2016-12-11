package com.jahop.api;

import java.util.Properties;

/**
 * Created by Pavel on 9/1/2016.
 */
public interface ClientFactory {
    Client create(Properties props);
}
