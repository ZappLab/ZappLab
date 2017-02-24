package com.jahop.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ServerTest {
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
        // init connections
        final DummySender[] senders = new DummySender[100];
        for (int i = 0; i < senders.length; i++) {
            senders[i] = new DummySender(i, serverAddress);
            senders[i].connect();
        }

        final CountDownLatch latch = new CountDownLatch(5 * senders.length);
        eventHandler.setLatch(latch);

        // send dummy data
        for (DummySender sender : senders) {
            sender.send(new byte[] {11,12,13});
            sender.send(new byte[] {21,22,23});
            sender.send(new byte[] {31,32,33});
            sender.send(new byte[] {41,42,43});
            sender.send(new byte[] {51,52,53});
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
            } else {
                throw new RuntimeException();
            }
        }
    }
}