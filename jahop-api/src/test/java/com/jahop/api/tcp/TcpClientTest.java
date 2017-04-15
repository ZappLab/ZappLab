package com.jahop.api.tcp;

import com.google.protobuf.TextFormat;
import com.jahop.api.Client;
import com.jahop.common.msg.proto.Messages.Entry;
import com.jahop.common.msg.proto.Messages.EntrySet;
import com.jahop.common.msg.proto.Messages.Update;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.GenericGroovyApplicationContext;


public class TcpClientTest {
    private static final Logger log = LogManager.getLogger(TcpClientTest.class);

    private final Update.Builder updateBuilder = Update.newBuilder();
    private final EntrySet.Builder entrySetBuilder = EntrySet.newBuilder();
    private final Entry.Builder entryBuilder = Entry.newBuilder();

    private GenericGroovyApplicationContext context;
    private Client client;

    @Before
    public void setUp() throws Exception {
        context = new GenericGroovyApplicationContext();
        context.load("context.groovy");
        context.refresh();

        client = context.getBean(TcpClient.class);
    }

    @After
    public void tearDown() throws Exception {
        context.close();
    }

    @Test
    public void testSend() throws Exception {
        // small
        {
            for (byte i = 0; i < 8; i++) {
                final Update update = createUpdate(String.valueOf(i));
                log.info("Sending: {}", TextFormat.shortDebugString(update));
                client.send(update);
            }
        }
        //large one
        {
            final String[] paths = new String[500];
            for (int i = 0; i < paths.length; i++) {
                paths[i] = String.valueOf(i);
            }
            final Update update = createUpdate(paths);
            log.info("Sending: {}", TextFormat.shortDebugString(update));
            client.send(update);
        }
        Thread.sleep(500);
        // small
        {
            for (byte i = 8; i < 12; i++) {
                final Update update = createUpdate(String.valueOf(i));
                log.info("Sending: {}", TextFormat.shortDebugString(update));
                client.send(update);
            }
        }
        Thread.sleep(500);
    }

    private Update createUpdate(final String... paths) {
        updateBuilder.clear();
        updateBuilder.setAuthor("Pavel");
        updateBuilder.setComment("No Comments");
        for (String path : paths) {
            updateBuilder.addEntrySet(entrySetBuilder.setPath(path).build());
        }
        return updateBuilder.build();
    }
}
