package com.jahop.server;

import com.jahop.common.msg.Message;

import java.util.*;

public class MessagesQueue {
    private final HashMap<Source, ArrayList<Message>> messages = new HashMap<>();
    private final HashSet<Source> sources = new HashSet<>();

    public void registerSource(final Source source) {
        synchronized (messages) {
            messages.put(source, new ArrayList<>());
        }
    }

    public ArrayList<Message> unregisterSource(final Source source) {
        synchronized (messages) {
            sources.remove(source);
            return messages.remove(source);
        }
    }

    public void pushMessage(final Source source, final Message message) {
        synchronized (messages) {
            final ArrayList<Message> messages = this.messages.get(source);
            if (messages == null) {
                throw new RuntimeException("Unknown source: " + source);
            }
            messages.add(message);
            sources.add(source);
        }
    }

    public ArrayList<Message> drainMessages(final Source source) {
        synchronized (messages) {
            return messages.put(source, new ArrayList<>());
        }
    }

    public Collection<Source> drainSources() {
        synchronized (messages) {
            if (sources.isEmpty()) {
                return Collections.emptyList();
            }
            final ArrayList<Source> channels = new ArrayList<>(sources);
            sources.clear();
            return channels;
        }
    }

    public void clear() {
        synchronized (messages) {
            messages.clear();
            sources.clear();
        }
    }
}
