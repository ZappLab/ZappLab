package com.jahop.server.connectors.tcp;

import com.jahop.common.msg.Message;
import com.jahop.server.Source;

import java.util.*;
import java.util.function.Consumer;

public class MessagesQueue {
    private final Map<Source, Deque<Message>> messages = new HashMap<>();
    private Set<Source> sources = new HashSet<>();
    private Set<Source> drainSources = new HashSet<>();

    public void registerSource(final Source source) {
        synchronized (messages) {
            messages.put(source, new LinkedList<>());
        }
    }

    public Deque<Message> unregisterSource(final Source source) {
        synchronized (messages) {
            sources.remove(source);
            return messages.remove(source);
        }
    }

    public void addLast(final Source source, final Message message) {
        synchronized (messages) {
            final Deque<Message> messages = this.messages.get(source);
            if (messages == null) {
                throw new RuntimeException("Unknown source: " + source);
            }
            messages.addLast(message);
            sources.add(source);
        }
    }

    public Message pollMessage(final Source source) {
        synchronized (messages) {
            final Deque<Message> messages = this.messages.get(source);
            if (messages == null) {
                throw new RuntimeException("Unknown source: " + source);
            }
            return messages.poll();
        }
    }

    public void drainSources(final Consumer<Source> consumer) {
        synchronized (messages) {
            if (sources.isEmpty()) {
                return;
            }
            final Set<Source> swap = sources;
            sources = drainSources;
            drainSources = swap;
        }
        final Iterator<Source> it = drainSources.iterator();
        while (it.hasNext()) {
            consumer.accept(it.next());
            it.remove();
        }
    }

    public void clear() {
        synchronized (messages) {
            messages.clear();
            sources.clear();
        }
    }
}
