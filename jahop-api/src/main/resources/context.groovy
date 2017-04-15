import com.jahop.api.tcp.TcpClientFactory

Properties clientProperties = new Properties()
getClass().getResource('client.properties').withInputStream {
    stream -> clientProperties.load(stream)
}

beans {
    clientFactory(TcpClientFactory)

    client(clientFactory: "create") { bean ->
        bean.constructorArgs = [clientProperties]
        bean.initMethod = "start"
        bean.destroyMethod = "stop"
    }
}