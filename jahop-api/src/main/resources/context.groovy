import com.jahop.api.Client
import com.jahop.common.msg.proto.Messages

class DummySender {
    Client client;

    void start() {
        final Messages.SnapshotRequest.Builder builder = Messages.SnapshotRequest.newBuilder();
        for (byte i = 0; i < 8; i++) {
            builder.clear();
            builder.setClientRevision(1);
            builder.addPath(String.valueOf(i));
            final Messages.SnapshotRequest request = builder.build();
            client.send(request);
        }
    }
}

beans {
    client(Client) { bean ->
        bean.initMethod = "start"
        bean.destroyMethod = "stop"
        remoteHost = "localhost"
        remotePort = 9090
    }

    dummySender(DummySender) { bean ->
        bean.initMethod = "start"
        client = client
    }
}