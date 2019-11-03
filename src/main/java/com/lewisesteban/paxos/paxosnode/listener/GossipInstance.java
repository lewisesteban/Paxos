package com.lewisesteban.paxos.paxosnode.listener;

public class GossipInstance {
    private long gossipNumber;
    private long instance;

    public GossipInstance(long instance, long gossipNumber) {
        this.gossipNumber = gossipNumber;
        this.instance = instance;
    }

    long getGossipNumber() {
        return gossipNumber;
    }

    long getInstance() {
        return instance;
    }
}
