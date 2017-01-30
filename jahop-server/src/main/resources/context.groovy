import com.jahop.server.ServerBootstrap

beans {
    server(ServerBootstrap) { bean ->
        bean.initMethod = "start"
        bean.destroyMethod = "stop"
        port = 9090
        sourceId = 1
    }

}