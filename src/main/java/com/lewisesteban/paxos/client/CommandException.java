package com.lewisesteban.paxos.client;

import java.io.Serializable;

public class CommandException extends Throwable {
    private Long instanceThatMayHaveBeenInitiated;
    private Serializable commandData;
    private long commandNb;

    CommandException(Long instanceThatMayHaveBeenInitiated, Serializable commandData, long commandNb, Throwable cause) {
        super(cause);
        this.instanceThatMayHaveBeenInitiated = instanceThatMayHaveBeenInitiated;
        this.commandData = commandData;
        this.commandNb = commandNb;
    }

    Long getInstanceThatMayHaveBeenInitiated() {
        return instanceThatMayHaveBeenInitiated;
    }

    public Serializable getCommandData() {
        return commandData;
    }

    long getCommandNb() {
        return commandNb;
    }
}
