package com.lewisesteban.paxos.virtualnet;

public interface VirtualNode {

    int getAddress();
    int getRack();
    boolean isRunning();
    void shutDown();
    void start();
    void kill();
}
