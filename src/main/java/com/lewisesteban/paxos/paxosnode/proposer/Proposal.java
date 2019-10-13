package com.lewisesteban.paxos.paxosnode.proposer;

import java.io.Serializable;

public class Proposal implements java.io.Serializable {

    private ID id;
    private Serializable command;

    public Proposal(Serializable command, ID id) {
        this.command = command;
        this.id = id;
    }

    public ID getId() {
        return id;
    }

    public Serializable getCommand() {
        return command;
    }

    public static class ID implements java.io.Serializable {

        private int nodeId;
        private int nodePropNb;

        ID(int nodeId, int nodeReqNb) {
            this.nodeId = nodeId;
            this.nodePropNb = nodeReqNb;
        }

        public boolean isGreaterThan(ID other) {
            return (nodePropNb > other.nodePropNb || (nodePropNb == other.nodePropNb && nodeId > other.nodeId));
        }

        public void set(ID other) {
            this.nodeId = other.nodeId;
            this.nodePropNb = other.nodePropNb;
        }

        public static ID noProposal() {
            return new ID(-1, -1);
        }

        @Override
        public String toString() {
            return "#" + nodePropNb + " #" + nodeId;
        }
    }
}
