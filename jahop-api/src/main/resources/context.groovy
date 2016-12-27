import com.jahop.api.Sender
import com.jahop.api.tcp.TcpClient
import com.jahop.api.tcp.TcpClientFactory
import com.jahop.common.msg.proto.Messages

class DummySender {
    TcpClient client;

    void send() {
        final Messages.SnapshotRequest.Builder builder = Messages.SnapshotRequest.newBuilder();
        client.connect();
        try {
            final Sender sender = client.getSender("dummy")
            // small
            for (byte i = 0; i < 8; i++) {
                builder.clear()
                builder.setClientRevision(1)
                builder.addPath(String.valueOf(i))
                sender.send(builder.build().toByteArray())
            }
            //large one
            builder.clear()
            builder.setClientRevision(1)
            for (int i = 0; i < 600; i++) {
                builder.addPath(String.valueOf(i))
            }
            sender.send(builder.build().toByteArray())
            // small
            Thread.sleep(500)
            for (byte i = 8; i < 12; i++) {
                builder.clear()
                builder.setClientRevision(1)
                builder.addPath(String.valueOf(i))
                sender.send(builder.build().toByteArray())
            }
        } finally {
            client.close()
        }
    }
}

beans {
    clientFactory(TcpClientFactory)

    client(clientFactory: "create") { bean ->
        bean.constructorArgs=[["jahop.server.host":"localhost","jahop.server.port":"9090"]]
    }

    dummySender(DummySender) { bean ->
        bean.initMethod = "send"
        client = client
    }
}