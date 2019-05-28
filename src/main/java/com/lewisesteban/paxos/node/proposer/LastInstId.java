package com.lewisesteban.paxos.node.proposer;

import java.util.concurrent.atomic.AtomicInteger;

public class LastInstId {

    private AtomicInteger id;

    LastInstId(int id) {
        this.id = new AtomicInteger(id);
    }

    int get() {
        return id.get();
    }

    synchronized void increaseTo(int id) {
        if (this.id.get() < id) {
            this.id.set(id);
        }
    }

    int getAndIncrement() {
        return id.getAndIncrement();
    }
}
