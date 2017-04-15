package com.jahop.api.tcp;

import com.jahop.api.Client;
import com.jahop.api.ClientFactory;
import com.jahop.api.Environment;
import com.jahop.api.Transport;
import com.jahop.common.msg.proto.Messages.Entry;
import com.jahop.common.msg.proto.Messages.EntrySet;
import com.jahop.common.msg.proto.Messages.Update;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Function;

public class TcpClientTest {
    private static final Logger log = LogManager.getLogger(TcpClientTest.class);

    private final Update.Builder updateBuilder = Update.newBuilder();
    private final EntrySet.Builder entrySetBuilder = EntrySet.newBuilder();
    private final Entry.Builder entryBuilder = Entry.newBuilder();

    private Client client;

    @Before
    public void setUp() throws Exception {
        final ClientFactory factory = ClientFactory.newInstance(Transport.TCP, Environment.LOCAL);
        Assert.assertTrue(factory instanceof TcpClientFactory);
        client = factory.create(1001);
        Assert.assertTrue(client instanceof TcpClient);
        client.start();

        log.info("### TcpClientTest started");
    }

    @After
    public void tearDown() throws Exception {
        client.stop();
        log.info("### TcpClientTest stopped");
    }

    @Test
    public void testSend() throws Exception {
        {
            // small
            for (byte i = 0; i < 8; i++) {
                createAndSend(path -> path + ".value", String.valueOf(i));
            }
        }
        {
            //large one
            final String[] paths = new String[500];
            for (int i = 0; i < paths.length; i++) {
                paths[i] = String.valueOf(i);
            }
            createAndSend(path -> path + ".value", paths);
        }
        {
            // small
            for (byte i = 8; i < 12; i++) {
                createAndSend(path -> path + ".value", String.valueOf(i));
            }
        }

        Thread.sleep(5000);
    }

    private void createAndSend(final Function<String, String> entryFunction, final String... paths) {
        updateBuilder.clear().setAuthor("Pavel").setComment("Test update");
        for (String path : paths) {
            entryBuilder.clear().setAction(Entry.Action.UPDATE)
                    .setKey("param").setValue(entryFunction.apply(path));
            entrySetBuilder.clear().setPath(path).addEntry(entryBuilder.build());
            updateBuilder.addEntrySet(entrySetBuilder.build());
        }
        final Update update = updateBuilder.build();

        client.send(update);
    }
}
