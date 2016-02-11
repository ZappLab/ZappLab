package com.zapplab.server;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import com.zapplab.server.events.LongEvent;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.RingBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class LongEventMain {
    public static void main(String[] args) throws Exception {
        // Executor that will be used to construct new threads for consumers
        final Executor executor = Executors.newCachedThreadPool();
        final LongEventFactory factory = new LongEventFactory();
        // Specify the size of the ring buffer, must be power of 2.
        final int bufferSize = 1024;

        // Construct the Disruptor
        final Disruptor<LongEvent> disruptor = new Disruptor<>(factory, bufferSize, executor, ProducerType.SINGLE, new BlockingWaitStrategy());

        // Connect the handler
//        disruptor.handleEventsWith((event, sequence, endOfBatch) -> System.out.println("Event: " + event.getValue()));
        disruptor.handleEventsWith(new LongEventHandler());



        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        final RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();

        final LongEventProducer producer = new LongEventProducer(ringBuffer);
        final ByteBuffer bb = ByteBuffer.allocate(8);
        for (long l = 0; l < 10; l++) {
            bb.putLong(0, l);
            producer.onData(bb);
            Thread.sleep(100);
        }

        disruptor.shutdown();
    }
}