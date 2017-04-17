package com.jahop.console;

public enum Command {
    connect (
            "connect <transport> <env>    Connect to the specified server instance\n" +
            "                             <transport>=TCP|UDP\n" +
            "                             <env>=LOCAL|DEV|UAT|PROD"
    ),
    disconnect (
            "disconnect                   Disconnect from server"
    ),
    subscribe (
            "subscribe <path>             Select <path> keys and subscribe for updates"
    ),
    unsubscribe (
            "unsubscribe <path>           Unsubscribe from <path> updates"
    ),
    select (
            "select <path>                Select <path> keys"
    ),
    update (
            "update <path> <key> <value>  Update value for <path>.<key>"
    ),
    delete (
            "delete <path> <key>          Delete <path>.<key>"
    ),
    help (
            "help [<command>]             Print general help or command usage information"
    ),
    version (
            "version                      Print version information"
    ),
    exit (
            "exit                         Exit console"
    );

    private final String description;

    Command(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public static Command parse(String str) {
        for (Command command : values()) {
            if (str.equals(command.toString())) {
                return command;
            }
        }
        return null;
    }
}
