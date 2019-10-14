package com.lewisesteban.paxos.paxosnode;

import java.io.Serializable;
import java.util.Random;

@SuppressWarnings("unused")
public class Command implements Serializable {

    private Serializable data;
    private String clientId;
    private long clientCmdNb;

    /**
     * The clientId and clientCmdNb are only used to check if two commands are equal
     */
    public Command(Serializable data, String clientId, long clientCmdNb) {
        this.data = data;
        this.clientId = clientId;
        this.clientCmdNb = clientCmdNb;
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

    public boolean equals(Command other) {
        return other.clientId.equals(clientId) && other.clientCmdNb == clientCmdNb;
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

        public static Command makeRandom(Serializable data) {
            return new Command(data, "", new Random().nextLong());
        }
    }
}
