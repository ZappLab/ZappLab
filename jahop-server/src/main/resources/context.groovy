import com.jahop.server.Server

beans {
    server(Server) { bean ->
        bean.initMethod = "start"
        bean.destroyMethod = "stop"
        port = 9090
        sourceId = 1
    }

}