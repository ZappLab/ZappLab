package com.jahop.server.connectors.tcp;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jahop.common.msg.MessageProvider;
import com.jahop.server.connectors.Connectors;
import com.jahop.server.handlers.EchoRequestHandler;
import com.jahop.server.msg.Request;
import com.jahop.server.msg.RequestFactory;
import com.jahop.server.msg.RequestProducer;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TcpConnectorTest {
    private static final Logger log = LogManager.getLogger(TcpConnectorTest.class);
    private static final long TIMEOUT_SECONDS = 10;
    private final InetSocketAddress serverAddress = new InetSocketAddress(12321);
    private Disruptor<Request> disruptor;
    private Connectors connectors;
    private long totalTimeNs;
    private long testTimeNs;

    @Before
    public void setUp() throws Exception {
        totalTimeNs = System.nanoTime();
        final MessageProvider messageProvider = new MessageProvider(1, 0);
        disruptor = new Disruptor<>(
                new RequestFactory(),
                1024,
                new ThreadFactoryBuilder().setNameFormat("worker-thread-%d").build(),
                ProducerType.SINGLE,
                new BlockingWaitStrategy()
        );
        connectors = new Connectors(new TcpConnector(serverAddress, new RequestProducer(disruptor.getRingBuffer())));
        final EchoRequestHandler eventHandler = new EchoRequestHandler(connectors, messageProvider);
        disruptor.handleEventsWith(eventHandler);

        disruptor.start();
        connectors.start();

        testTimeNs = System.nanoTime();
    }

    @After
    public void tearDown() throws Exception {
        log.info("## Test took: {} ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - testTimeNs));
        connectors.stop();
        disruptor.shutdown();
        log.info("#### Total time: {} ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - totalTimeNs));
    }

    @Test
    public void testEcho() throws Exception {
        final long timeoutNs = TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        final int sendersCount = 50;
        final int messagesCount = 100;

        // init connections and messages
        final Selector selector = Selector.open();
        final ByteBuffer[][] messages = new ByteBuffer[sendersCount][messagesCount];
        final DummyClient[] senders = new DummyClient[sendersCount];
        for (int i = 0; i < sendersCount; i++) {
            final DummyClient sender = new DummyClient(i, serverAddress);
            sender.connect(selector);
            senders[i] = sender;
            for (int j = 0; j < messagesCount; j++) {
                final String text = String.format("sourceId: %d, messageId: %d", i, j);
                messages[i][j] = sender.wrapPayload(text.getBytes());
            }
        }

        final AtomicInteger responsesCount = new AtomicInteger(messagesCount * sendersCount);

        // send messages concurrently
        final long startNs = System.nanoTime();
        while ((System.nanoTime() - startNs) < timeoutNs && responsesCount.get() > 0) {
            if (selector.select(100) > 0) {
                final Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
                while (selectedKeys.hasNext()) {
                    final SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    final DummyClient client = (DummyClient) key.attachment();
                    // Check what event is available and deal with it
                    if (key.isReadable()) {
                        client.receive(message -> responsesCount.decrementAndGet());
                    } else if (key.isWritable()) {
                        final int sourceId = client.getSourceId();
                        final ByteBuffer[] clientMessages = messages[sourceId];
                        client.send(clientMessages);
                    }
                }
            }
        }

        Assert.assertEquals("Responses missing", 0, responsesCount.get());

        // close all connections
        for (DummyClient sender : senders) {
            sender.close();
        }
    }
}