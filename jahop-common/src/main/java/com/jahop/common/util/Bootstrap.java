package com.jahop.common.util;

import org.springframework.context.support.GenericGroovyApplicationContext;

/**
 * Created by Pavel on 8/13/2016.
 */
public class Bootstrap {
    public static void main(String[] args) throws Exception {
        final GenericGroovyApplicationContext context = new GenericGroovyApplicationContext();
        context.load("context.groovy");
        context.refresh();
    }
}
