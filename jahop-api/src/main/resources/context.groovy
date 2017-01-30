import com.jahop.api.Sender
import com.jahop.api.tcp.TcpClient
import com.jahop.api.tcp.TcpClientFactory
import com.jahop.common.msg.MessageFactory
import com.jahop.common.msg.proto.Messages

class DummySender {
    TcpClient client;

    void start() {
        final Messages.Update.Builder updateBuilder = Messages.Update.newBuilder();
        final Messages.EntrySet.Builder entrySetBuilder = Messages.EntrySet.newBuilder();
        final Messages.Entry.Builder entryBuilder = Messages.Entry.newBuilder();
        client.connect();
        try {
            final Sender sender = client.getSender("dummy")
            // small
            for (byte i = 0; i < 8; i++) {
                updateBuilder.clear()
                updateBuilder.setAuthor("Pavel")
                updateBuilder.setComment("No Comments")
                updateBuilder.addEntrySet(entrySetBuilder.setPath(String.valueOf(i)).build())
                sender.send(updateBuilder.build().toByteArray())
            }
            //large one
            updateBuilder.clear()
            updateBuilder.setAuthor("Pavel")
            updateBuilder.setComment("No Comments")
            for (int i = 0; i < 600; i++) {
                updateBuilder.addEntrySet(entrySetBuilder.setPath(String.valueOf(i)).build())
            }
            sender.send(updateBuilder.build().toByteArray())
            // small
            Thread.sleep(500)
            for (byte i = 8; i < 12; i++) {
                updateBuilder.clear()
                updateBuilder.setAuthor("Pavel")
                updateBuilder.setComment("No Comments")
                updateBuilder.addEntrySet(entrySetBuilder.setPath(String.valueOf(i)).build())
                sender.send(updateBuilder.build().toByteArray())
            }
            Thread.sleep(5000)
        } finally {
            client.close()
        }
    }
}

beans {
    clientFactory(TcpClientFactory)

    client(clientFactory: "create") { bean ->
        bean.constructorArgs=[[
                                      "jahop.server.host":"localhost",
                                      "jahop.server.port":"9090",
                                      "jahop.client.id":"13"
                              ]]
    }

    dummySender(DummySender) { bean ->
        bean.initMethod = "start"
        client = client
    }
}