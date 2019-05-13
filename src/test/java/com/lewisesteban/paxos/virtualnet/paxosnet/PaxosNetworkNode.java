package com.lewisesteban.paxos.virtualnet.paxosnet;

import com.lewisesteban.paxos.virtualnet.VirtualNetNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;

public class PaxosNetworkNode implements VirtualNetNode {

    private PaxosServer paxosSrv;
    private boolean isRunning = false;
    private int rack = 0;

    public PaxosNetworkNode(PaxosServer paxosSrv, int rack) {
        this.paxosSrv = paxosSrv;
        this.rack = rack;
    }

    public PaxosNetworkNode(PaxosServer paxosSrv) {
        this.paxosSrv = paxosSrv;
    }

    public PaxosServer getPaxosSrv() {
        return paxosSrv;
    }

    public int getAddress() {
        return paxosSrv.getId();
    }

    public int getRack() {
        return rack;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public void shutDown() {
        paxosSrv.stop();
        isRunning = false;
    }

    public void kill() {
        paxosSrv.kill();
        isRunning = false;
    }

    public void start() {
        paxosSrv.start();
        isRunning = true;
    }
}
