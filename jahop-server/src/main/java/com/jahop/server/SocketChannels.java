package com.jahop.server;

import com.jahop.common.msg.Message;

import java.nio.channels.SocketChannel;
import java.util.*;

public class SocketChannels {
    private final HashMap<SocketChannel, ArrayList<Message>> messages = new HashMap<>();
    private final HashSet<SocketChannel> pendingChannels = new HashSet<>();

    public void registerChannel(final SocketChannel channel) {
        synchronized (messages) {
            messages.put(channel, new ArrayList<>());
        }
    }

    public ArrayList<Message> unregisterChannel(final SocketChannel channel) {
        synchronized (messages) {
            pendingChannels.remove(channel);
            return messages.remove(channel);
        }
    }

    public void putMessage(final SocketChannel channel, final Message message) {
        synchronized (messages) {
            final ArrayList<Message> messages = this.messages.get(channel);
            if (messages == null) {
                throw new RuntimeException("Unknown channel: " + channel);
            }
            messages.add(message);
            pendingChannels.add(channel);
        }
    }

    public ArrayList<Message> drainMessages(final SocketChannel channel) {
        synchronized (messages) {
            return messages.put(channel, new ArrayList<>());
        }
    }

    public Collection<SocketChannel> drainPendingChannels() {
        synchronized (messages) {
            if (pendingChannels.isEmpty()) {
                return Collections.emptyList();
            }
            final ArrayList<SocketChannel> channels = new ArrayList<>(pendingChannels);
            pendingChannels.clear();
            return channels;
        }
    }
}
