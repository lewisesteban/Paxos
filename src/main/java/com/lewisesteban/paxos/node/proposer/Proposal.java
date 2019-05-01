package com.lewisesteban.paxos.node.proposer;

import java.io.Serializable;

public class Proposal implements Serializable {

    private ID id;
    private Serializable data;

    public Proposal(Serializable data, ID id) {
        this.data = data;
        this.id = id;
    }

    public ID getId() {
        return id;
    }

    public Serializable getData() {
        return data;
    }

    void setData(Proposal other) {
        data = other.data;
    }

    public static class ID implements Serializable {

        private int nodeId;
        private long nodePropNb;

        ID(int nodeId, long nodeReqNb) {
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
    }
}
