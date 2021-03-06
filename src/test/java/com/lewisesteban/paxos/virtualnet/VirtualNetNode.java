package com.lewisesteban.paxos.virtualnet;

/**
 * A node on the virtual network, which contains all network-related information and commands.
 */
public interface VirtualNetNode {

    Network.Address getAddress();
    int getRack();
    boolean isRunning();
    void shutDown();
    void start();
    void kill();
}
