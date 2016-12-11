import com.jahop.api.Sender
import com.jahop.api.tcp.TcpClient
import com.jahop.api.tcp.TcpClientFactory
import com.jahop.common.msg.proto.Messages

class DummySender {
    TcpClient client;

    void send() {
        final Messages.SnapshotRequest.Builder builder = Messages.SnapshotRequest.newBuilder();
        client.connect();
        final Sender sender = client.getSender("dummy")
        for (byte i = 0; i < 8; i++) {
            builder.clear()
            builder.setClientRevision(1)
            builder.addPath(String.valueOf(i))
            final Messages.SnapshotRequest request = builder.build()
            sender.send(request.toByteArray())
        }
        client.close()
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