package com.jahop.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jahop.common.msg.Message;
import com.jahop.common.msg.MessageFactory;
import com.jahop.common.util.Sequencer;
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
import java.util.Locale;
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
    private long totalTimeNs;
    private long testTimeNs;

    @Before
    public void setUp() throws Exception {
        Locale.setDefault(Locale.US);
        totalTimeNs = System.nanoTime();
        final MessageFactory messageFactory = new MessageFactory(1, new Sequencer(0));
        eventHandler = new RequestEventHandler(messageFactory);
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
        testTimeNs = System.nanoTime();
    }

    @After
    public void tearDown() throws Exception {
        log.info("## Test took: {} ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - testTimeNs));
        server.stop();
        disruptor.shutdown();
        log.info("#### Total time: {} ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - totalTimeNs));
    }

    @Test
    public void test() throws Exception {
        final int sendersCount = 50;
        final int messagesCount = 4;

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
        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

        // close all connections
        for (DummySender sender : senders) {
            sender.close();
        }
    }


    static class RequestEventHandler implements EventHandler<Request> {
        private final MessageFactory messageFactory;
        private volatile CountDownLatch latch;

        public RequestEventHandler(MessageFactory messageFactory) {
            this.messageFactory = messageFactory;
        }

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onEvent(Request event, long sequence, boolean endOfBatch) throws Exception {
            if (event.isReady()) {
                latch.countDown();
                final Message message = event.getMessage();
                final String text = new String(message.getPayload(), 0, message.getPayloadSize());
                log.info("{}: received '{}'", event.getSocketChannel().getRemoteAddress(), text);

                final Message response = messageFactory.createPayload(0, message.getRequestId(), text.getBytes());
                event.sendResponse(response);
            } else {
                throw new RuntimeException("Broken message");
            }
        }
    }
}