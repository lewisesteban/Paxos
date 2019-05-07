package com.lewisesteban.paxos.virtualnet.node;

import com.lewisesteban.paxos.PaxosNode;
import com.lewisesteban.paxos.virtualnet.VirtualNode;

public class PaxosNodeWrapper implements VirtualNode {

    private PaxosNode paxosNode;
    private boolean isRunning = false;
    private int rack = 0;

    public PaxosNodeWrapper(PaxosNode paxosNode, int rack) {
        this.paxosNode = paxosNode;
        this.rack = rack;
    }

    public PaxosNodeWrapper(PaxosNode paxosNode) {
        this.paxosNode = paxosNode;
    }

    public PaxosNode getPaxosNode() {
        return paxosNode;
    }

    public int getAddress() {
        return paxosNode.getId();
    }

    public int getRack() {
        return rack;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public void shutDown() {
        paxosNode.stop();
        isRunning = false;
    }

    public void kill() {
        paxosNode.stop();
        isRunning = false;
    }

    public void start() {
        paxosNode.start();
        isRunning = true;
    }
}
