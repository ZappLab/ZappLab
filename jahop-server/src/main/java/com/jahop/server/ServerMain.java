package com.jahop.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jahop.common.msg.Request;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.RingBuffer;

import java.util.concurrent.ThreadFactory;

public class ServerMain {
    public static void main(String[] args) throws Exception {
        // Executor that will be used to construct new threads for consumers
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("server-main-thread").build();
        final RequestFactory factory = new RequestFactory();
        // Specify the size of the ring buffer, must be power of 2.
        final int bufferSize = 1024;

        // Construct the Disruptor
        final Disruptor<Request> disruptor = new Disruptor<>(factory, bufferSize, threadFactory, ProducerType.SINGLE, new BlockingWaitStrategy());

        // Connect the handler
        disruptor.handleEventsWith(new RequestHandler());



        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        final RingBuffer<Request> ringBuffer = disruptor.getRingBuffer();

        final RequestProducer producer = new RequestProducer(ringBuffer);
        final ServerLoop serverLoop = new ServerLoop(producer);
        serverLoop.start();

        disruptor.shutdown();
    }
}