package com.lewisesteban.paxos.client;

public class CommandException extends Throwable {
    private Long instanceThatMayHaveBeenInitiated;

    CommandException(Long instanceThatMayHaveBeenInitiated, Throwable cause) {
        super(cause);
        this.instanceThatMayHaveBeenInitiated = instanceThatMayHaveBeenInitiated;
    }

    public Long getInstanceThatMayHaveBeenInitiated() {
        return instanceThatMayHaveBeenInitiated;
    }
}
