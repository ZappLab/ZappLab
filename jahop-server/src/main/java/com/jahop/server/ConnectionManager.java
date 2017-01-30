package com.jahop.server;

import com.jahop.common.msg.Message;

import java.nio.channels.SocketChannel;
import java.util.*;

public class ConnectionManager {
    private final HashMap<SocketChannel, ArrayList<Message>> channelMessages = new HashMap<>();
    private final HashSet<SocketChannel> pendingChannels = new HashSet<>();

    public void addChannel(final SocketChannel channel) {
        synchronized (channelMessages) {
            channelMessages.put(channel, new ArrayList<>());
        }
    }

    public ArrayList<Message> removeChannel(final SocketChannel channel) {
        synchronized (channelMessages) {
            return channelMessages.remove(channel);
        }
    }

    public void addChannelMessage(final SocketChannel channel, final Message message) {
        synchronized (channelMessages) {
            final ArrayList<Message> messages = channelMessages.get(channel);
            if (messages == null) {
                throw new RuntimeException("Unknown connection: " + channel);
            }
            messages.add(message);
            pendingChannels.add(channel);
        }
    }

    public ArrayList<Message> pollChannelMessages(final SocketChannel channel) {
        synchronized (channelMessages) {
            return channelMessages.put(channel, new ArrayList<>());
        }
    }

    public Collection<SocketChannel> pollPendingChannels() {
        synchronized (channelMessages) {
            if (pendingChannels.isEmpty()) {
                return Collections.emptyList();
            }
            final ArrayList<SocketChannel> connections = new ArrayList<>(pendingChannels);
            pendingChannels.clear();
            return connections;
        }
    }
}
