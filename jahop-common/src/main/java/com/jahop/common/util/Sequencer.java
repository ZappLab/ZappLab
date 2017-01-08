package com.jahop.common.util;

import java.util.concurrent.atomic.AtomicLong;

public class Sequencer {
    private final AtomicLong sequencer;

    public Sequencer() {
        sequencer = new AtomicLong(System.currentTimeMillis());
    }

    public long next() {
        return sequencer.incrementAndGet();
    }

    @Override
    public String toString() {
        return sequencer.toString();
    }
}
