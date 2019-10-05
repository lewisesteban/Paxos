package com.lewisesteban.paxos.paxosnode;

import java.io.Serializable;

public class Command implements Serializable {

    private Id id;
    private Serializable data;

    public Command(int clientId, long commandNb, Serializable data) {
        this.id = new Id(clientId, commandNb);
        this.data = data;
    }

    public Id getId() {
        return id;
    }

    public int getClientId() {
        return id.clientId;
    }

    public long getCommandNb() {
        return id.commandNb;
    }

    public Serializable getData() {
        return data;
    }

    @Override
    public String toString() {
        return "client " + id.clientId + " command " + id.commandNb;
    }

    public class Id implements Comparable<Id> {

        private int clientId;
        private long commandNb;

        Id(int clientId, long commandNb) {
            this.clientId = clientId;
            this.commandNb = commandNb;
        }

        @Override
        public int compareTo(Id o) {
            if (clientId > o.clientId) {
                return 1;
            } else if (clientId < o.clientId) {
                return -1;
            } else {
                return Long.compare(commandNb, o.commandNb);
            }
        }
    }
}
