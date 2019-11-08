package com.lewisesteban.paxos.virtualnet.paxosnet;

import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.VirtualNetNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;

public class PaxosNetworkNode implements VirtualNetNode {

    private PaxosServer paxosSrv;
    private boolean isRunning = false;
    private int rack = 0;
    private Network.Address address;

    public PaxosNetworkNode(PaxosServer paxosSrv, int rack) {
        this.paxosSrv = paxosSrv;
        this.rack = rack;
        this.address = new Network.Address(paxosSrv.getFragmentId(), paxosSrv.getId());
    }

    public PaxosNetworkNode(PaxosServer paxosSrv) {
        this.paxosSrv = paxosSrv;
    }

    public PaxosServer getPaxosSrv() {
        return paxosSrv;
    }

    public Network.Address getAddress() {
        return address;
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
        isRunning = false;
        paxosSrv.kill();
    }

    public void start() {
        paxosSrv.start();
        isRunning = true;
    }
}
