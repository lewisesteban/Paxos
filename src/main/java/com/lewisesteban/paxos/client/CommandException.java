package com.lewisesteban.paxos.client;

import java.io.Serializable;

public class CommandException extends Throwable {
    private Long instanceThatMayHaveBeenInitiated;
    private Serializable commandData;
    private long commandNb;

    CommandException(Long instanceThatMayHaveBeenInitiated, Serializable commandData, long commandNb, Throwable cause) {
        super(cause);
        this.instanceThatMayHaveBeenInitiated = instanceThatMayHaveBeenInitiated;
    }

    Long getInstanceThatMayHaveBeenInitiated() {
        return instanceThatMayHaveBeenInitiated;
    }

    Serializable getCommandData() {
        return commandData;
    }

    long getCommandNb() {
        return commandNb;
    }
}
