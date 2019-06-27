package com.lewisesteban.paxos.node.proposer;

import com.lewisesteban.paxos.Command;

import java.io.Serializable;

public class Proposal implements Serializable {

    private ID id;
    private Command command;

    public Proposal(Command command, ID id) {
        this.command = command;
        this.id = id;
    }

    public ID getId() {
        return id;
    }

    public Command getCommand() {
        return command;
    }

    public static class ID implements Serializable {

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
