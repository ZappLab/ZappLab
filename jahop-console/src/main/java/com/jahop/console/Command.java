package com.jahop.console;

public enum Command {
    help, version, exit;

    public static Command parse(String str) {
        for (Command command : values()) {
            if (str.equals(command.toString())) {
                return command;
            }
        }
        return null;
    }
}
