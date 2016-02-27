package com.jahop.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jahop.common.msg.Request;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.RingBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadFactory;

public class ServerMain {
    public static void main(String[] args) throws Exception {
        // Executor that will be used to construct new threads for consumers
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("server-main-thread").build();
        final ServerEventFactory factory = new ServerEventFactory();
        // Specify the size of the ring buffer, must be power of 2.
        final int bufferSize = 1024;

        // Construct the Disruptor
        final Disruptor<Request> disruptor = new Disruptor<>(factory, bufferSize, threadFactory, ProducerType.SINGLE, new BlockingWaitStrategy());

        // Connect the handler
        disruptor.handleEventsWith(new ServerEventHandler());



        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        final RingBuffer<Request> ringBuffer = disruptor.getRingBuffer();

        final ServerEventProducer producer = new ServerEventProducer(ringBuffer);
        final ByteBuffer bb = ByteBuffer.allocate(16);
        for (long l = 0; l < 10; l++) {
            bb.putLong(0, l);
            bb.putLong(8, 42);
            producer.onData(bb);
            Thread.sleep(100);
        }

        disruptor.shutdown();
    }
}