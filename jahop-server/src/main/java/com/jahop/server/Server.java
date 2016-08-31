package com.jahop.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ThreadFactory;

/**
 * Created by Pavel on 8/27/2016.
 */
public class Server {
    private static Logger log = LogManager.getLogger(Server.class);

    private int port = 9090;
    private ServerLoop serverLoop;

    // >>> Server request infrastructure
    private final ThreadFactory requestThreadFactory = new ThreadFactoryBuilder().setNameFormat("request-thread-%d").build();
    private final RequestFactory requestFactory = new RequestFactory();
    // Specify the size of the request ring buffer, must be power of 2.
    private int requestBufferSize = 1024;
    private Disruptor<Request> requestDisruptor;

    // <<< Server response infrastructure
    private final ThreadFactory responseThreadFactory = new ThreadFactoryBuilder().setNameFormat("response-thread-%d").build();
//    final RequestFactory responseFactory = new RequestFactory();
    // Specify the size of the response ring buffer, must be power of 2.
    private int responseBufferSize = 1024;
    private Disruptor<Response> responseDisruptor;

    public void setPort(int port) {
        this.port = port;
    }

    public void setRequestBufferSize(int requestBufferSize) {
        this.requestBufferSize = requestBufferSize;
    }

    public void setResponseBufferSize(int responseBufferSize) {
        this.responseBufferSize = responseBufferSize;
    }

    public void start() throws IOException {
        // Construct response disruptor and message handler
        responseDisruptor = new Disruptor<>(Response::new, responseBufferSize, responseThreadFactory, ProducerType.SINGLE, new BlockingWaitStrategy());
        responseDisruptor.handleEventsWith(new ResponseHandler());
        // Start response processing threads
        responseDisruptor.start();

        // Construct request Disruptor and message handler
        requestDisruptor = new Disruptor<>(requestFactory, requestBufferSize, requestThreadFactory, ProducerType.SINGLE, new BlockingWaitStrategy());
        requestDisruptor.handleEventsWith(new RequestHandler());
        // Start request processing threads
        requestDisruptor.start();

        final RequestProducer producer = new RequestProducer(requestDisruptor.getRingBuffer());

        serverLoop = new ServerLoop(producer, port);
        serverLoop.start();
    }

    public void stop() {
        requestDisruptor.shutdown();
        serverLoop.stop();
        responseDisruptor.shutdown();
    }
}
