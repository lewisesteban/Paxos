package com.lewisesteban.paxos.paxosnode;

import java.io.Serializable;
import java.util.Random;

@SuppressWarnings("unused")
public class Command implements Serializable {

    private Serializable data;
    private String clientId;
    private long clientCmdNb;
    private boolean noOp = false;

    public Command() { }

    /**
     * The clientId and clientCmdNb are only used to check if two commands are equal
     */
    public Command(Serializable data, String clientId, long clientCmdNb) {
        this.data = data;
        this.clientId = clientId;
        this.clientCmdNb = clientCmdNb;
    }

    public static Command NoOpCommand() {
        Command command = new Command("NoOp", null, -1);
        command.noOp = true;
        return command;
    }

    public Serializable getData() {
        return data;
    }

    public String getClientId() {
        return clientId;
    }

    public long getClientCmdNb() {
        return clientCmdNb;
    }

    public boolean isNoOp() {
        return noOp;
    }

    public boolean equals(Command other) {
        if (noOp && other.noOp)
            return true;
        return noOp == other.noOp && other.clientId.equals(clientId) && other.clientCmdNb == clientCmdNb;
    }

    @Override
    public boolean equals(Object other) {
        if (other.getClass() == Command.class) {
            return equals((Command)other);
        } else {
            return super.equals(other);
        }
    }

    @Override
    public String toString() {
        if (data == null)
            return null;
        return data.toString();
    }

    public static class Factory {

        private String clientId;
        private long clientCmdNb = 0;

        public Factory(String clientId) {
            this.clientId = clientId;
        }

        public synchronized Command make(Serializable data) {
            return new Command(data, clientId, clientCmdNb++);
        }

        public synchronized  Command make(Serializable data, long cmdNb) {
            return new Command(data, clientId, cmdNb);
        }

        public static Command makeRandom(Serializable data) {
            return new Command(data, "", new Random().nextLong());
        }
    }
}
