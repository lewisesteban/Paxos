package com.lewisesteban.paxos.paxosnode.membership;

public class NodeHeartbeat {
    private boolean failed = false;
    private long counter;

    NodeHeartbeat(long counter) {
        this.counter = counter;
    }

    boolean isFailed() {
        return failed;
    }

    long getCounter() {
        return counter;
    }

    void setFailed(boolean failed) {
        this.failed = failed;
    }
}
