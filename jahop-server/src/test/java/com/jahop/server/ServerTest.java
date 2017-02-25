package com.jahop.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
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
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ServerTest {
    private static final Logger log = LogManager.getLogger(ServerTest.class);
    private final Random random = new Random(System.currentTimeMillis());
    private final InetSocketAddress serverAddress = new InetSocketAddress(12321);
    private RequestEventHandler eventHandler;
    private Disruptor<Request> disruptor;
    private Server server;

    @Before
    public void setUp() throws Exception {
        eventHandler = new RequestEventHandler();
        disruptor = new Disruptor<>(
                new RequestFactory(),
                1024,
                new ThreadFactoryBuilder().setNameFormat("worker-thread-%d").build(),
                ProducerType.SINGLE,
                new BlockingWaitStrategy()
        );
        disruptor.handleEventsWith(eventHandler);
        disruptor.start();

        server = new Server(new RequestProducer(disruptor.getRingBuffer()), serverAddress.getPort());
        server.start();
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
        disruptor.shutdown();
    }

    @Test
    public void test() throws Exception {
        final int sendersCount = 100;
        final int messagesCount = 5;

        // init connections and messages
        final ByteBuffer[][] messages = new ByteBuffer[sendersCount][messagesCount];
        final DummySender[] senders = new DummySender[sendersCount];
        for (int i = 0; i < sendersCount; i++) {
            final DummySender sender = new DummySender(i, serverAddress);
            sender.connect();
            senders[i] = sender;
            for (int j = 0; j < messagesCount; j++) {
                final String text = String.format("Message_%d_%d", i, j);
                messages[i][j] = sender.wrapPayload(text.getBytes());
            }
        }

        final CountDownLatch latch = new CountDownLatch(messagesCount * sendersCount);
        eventHandler.setLatch(latch);

        // send messages concurrently
        for (int j = 0; j < messagesCount; j++) {
            // send 1st part
            for (int i = 0; i < sendersCount; i++) {
                final DummySender sender = senders[i];
                final ByteBuffer message = messages[i][j];
                final int fullSize = message.remaining();
                final int partSize = random.nextInt(fullSize) + 1;
                message.limit(partSize);
                sender.send(message);
                message.limit(fullSize);
            }
            // send 2nd part (the rest)
            for (int i = 0; i < sendersCount; i++) {
                final DummySender sender = senders[i];
                final ByteBuffer message = messages[i][j];
                sender.send(message);
            }
        }

        // wait for all messages to hit server
        Assert.assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));

        // close all connections
        for (DummySender sender : senders) {
            sender.close();
        }
    }


    static class RequestEventHandler implements EventHandler<Request> {
        private volatile CountDownLatch latch;

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onEvent(Request event, long sequence, boolean endOfBatch) throws Exception {
            if (event.isReady()) {
                latch.countDown();
                log.info("{}: received {}", event.getSocketChannel().getRemoteAddress(), event.getMessage());
            } else {
                throw new RuntimeException();
            }
        }
    }
}