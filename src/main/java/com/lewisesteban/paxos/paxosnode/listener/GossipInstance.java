package com.lewisesteban.paxos.paxosnode.listener;

public class GossipInstance {
    private long gossipNumber;
    private Long instance;

    GossipInstance(Long instance, long gossipNumber) {
        this.gossipNumber = gossipNumber;
        this.instance = instance;
    }

    long getGossipNumber() {
        return gossipNumber;
    }

    Long getInstance() {
        return instance;
    }
}
