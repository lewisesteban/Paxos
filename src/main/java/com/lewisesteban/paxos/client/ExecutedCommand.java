package com.lewisesteban.paxos.client;

import java.io.Serializable;

public class ExecutedCommand {
    private Serializable commandData;
    private Serializable returnedData;

    ExecutedCommand(Serializable commandData, Serializable returnedData) {
        this.commandData = commandData;
        this.returnedData = returnedData;
    }

    public Serializable getCommandData() {
        return commandData;
    }

    public Serializable getReturnedData() {
        return returnedData;
    }
}
