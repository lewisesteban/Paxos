package com.lewisesteban.paxos.virtualnet.paxosnet;

import com.lewisesteban.paxos.storage.InterruptibleAccessorContainer;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.VirtualNetNode;

public class ClientNetNode implements VirtualNetNode {
    private static final int CLIENT_RACK = 1000000;
    private static final int CLIENT_CLUSTER = 1000000;

    private int clientNodeId;
    private boolean isRunning = false;

    /**
     * @param clientNodeId The client's node ID. Used only by the network. Must be different than that of any other
     *                     node (server or client) in the network.
     */
    public ClientNetNode(int clientNodeId) {
        this.clientNodeId = clientNodeId;
    }

    @Override
    public Network.Address getAddress() {
        return new Network.Address(CLIENT_CLUSTER, clientNodeId);
    }

    @Override
    public int getRack() {
        return CLIENT_RACK;
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public void shutDown() {
        isRunning = false;
    }

    @Override
    public void start() {
        isRunning = true;
    }

    @Override
    public void kill() {
        isRunning = false;
        InterruptibleAccessorContainer.interrupt(clientNodeId);
    }

    public static Network.Address address(int nodeId) {
        return new Network.Address(CLIENT_CLUSTER, nodeId);
    }
}
