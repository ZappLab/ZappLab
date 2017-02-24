package com.jahop.common.util;

import java.util.concurrent.atomic.AtomicLong;

public class Sequencer {
    private final AtomicLong sequencer;

    public Sequencer() {
        this(System.currentTimeMillis());
    }

    public Sequencer(final long initialValue) {
        sequencer = new AtomicLong(initialValue);
    }

    public long next() {
        return sequencer.incrementAndGet();
    }

    @Override
    public String toString() {
        return sequencer.toString();
    }
}
