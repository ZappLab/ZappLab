package com.jahop.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jahop.common.msg.Message;
import com.jahop.common.msg.MessageFactory;
import com.jahop.common.util.Sequencer;
import com.jahop.server.connectors.Connector;
import com.jahop.server.connectors.tcp.TcpConnector;
import com.jahop.server.msg.Request;
import com.jahop.server.msg.RequestFactory;
import com.jahop.server.msg.RequestProducer;
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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class ConnectorTest {
    private static final Logger log = LogManager.getLogger(ConnectorTest.class);
    private static final long TIMEOUT_SECONDS = 10;
    private final InetSocketAddress serverAddress = new InetSocketAddress(12321);
    private RequestEventHandler eventHandler;
    private Disruptor<Request> disruptor;
    private Connector connector;
    private long totalTimeNs;
    private long testTimeNs;

    @Before
    public void setUp() throws Exception {
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

        connector = new TcpConnector(serverAddress, new RequestProducer(disruptor.getRingBuffer()));
        connector.start();
        testTimeNs = System.nanoTime();
    }

    @After
    public void tearDown() throws Exception {
        log.info("## Test took: {} ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - testTimeNs));
        connector.stop();
        disruptor.shutdown();
        log.info("#### Total time: {} ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - totalTimeNs));
    }

    @Test
    public void test() throws Exception {
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

        final AtomicInteger requestsCount = new AtomicInteger(messagesCount * sendersCount);
        final AtomicInteger responsesCount = new AtomicInteger(messagesCount * sendersCount);
        eventHandler.setConsumer(message -> requestsCount.decrementAndGet());

        // send messages concurrently
        final long startNs = System.nanoTime();
        while ((System.nanoTime() - startNs) < timeoutNs && (requestsCount.get() > 0 || responsesCount.get() > 0)) {
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

        Assert.assertEquals("Requests missing",0, requestsCount.get());
        Assert.assertEquals("Responses missing", 0, responsesCount.get());

        // close all connections
        for (DummyClient sender : senders) {
            sender.close();
        }
    }


    static class RequestEventHandler implements EventHandler<Request> {
        private final MessageFactory messageFactory;
        private volatile Consumer<Message> consumer;

        public RequestEventHandler(MessageFactory messageFactory) {
            this.messageFactory = messageFactory;
        }

        public void setConsumer(Consumer<Message> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void onEvent(Request event, long sequence, boolean endOfBatch) throws Exception {
            if (event.isValid()) {
                final Message message = event.getMessage();
                final String text = new String(message.getPartBytes(), 0, message.getPayloadSize());
                log.info("{}: received '{}'", event.getSource(), text);

                if (consumer != null) {
                    consumer.accept(message);
                }

                final Message response = messageFactory.createPayload(0, message.getRequestId(), text.getBytes());
                event.getSource().send(response);
            } else {
                throw new RuntimeException("Broken message");
            }
        }
    }
}