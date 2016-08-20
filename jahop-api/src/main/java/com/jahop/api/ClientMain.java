package com.jahop.api;

import com.jahop.common.msg.proto.Messages;
import org.springframework.context.support.GenericGroovyApplicationContext;

/**
 * Created by Pavel on 8/13/2016.
 */
public class ClientMain {
    public static void main(String[] args) throws Exception {
        final GenericGroovyApplicationContext context = new GenericGroovyApplicationContext();
        context.load("context.groovy");
        context.refresh();

        final Client client = context.getBean(Client.class);
        client.start();
        final Messages.SnapshotRequest.Builder builder = Messages.SnapshotRequest.newBuilder();
        builder.setClientRevision(1);
        for (byte i = 0; i < 8; i++) {
            builder.clear();
            builder.addPath(String.valueOf(i));
            final Messages.SnapshotRequest request = builder.build();
            client.send(request);
        }
        Thread.sleep(10000);
        client.stop();
    }
}
