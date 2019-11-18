package com.lewisesteban.paxos.paxosnode.proposer;

import com.lewisesteban.paxos.paxosnode.Command;

public class Proposal implements java.io.Serializable {

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

    public static class ID implements java.io.Serializable {

        private int nodeId;
        private long nodePropNb;

        public ID(int nodeId, long nodeReqNb) {
            this.nodeId = nodeId;
            this.nodePropNb = nodeReqNb;
        }

        public int getNodeId() {
            return nodeId;
        }

        public long getNodePropNb() {
            return nodePropNb;
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
    }
}
