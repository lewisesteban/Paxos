package com.lewisesteban.paxos.paxosnode.proposer;

import java.util.concurrent.atomic.AtomicLong;

class LastInstId {

    private AtomicLong id;

    LastInstId(long id) {
        this.id = new AtomicLong(id);
    }

    long get() {
        return id.get();
    }

    synchronized void increaseTo(long id) {
        if (this.id.get() < id) {
            this.id.set(id);
        }
    }

    long getAndIncrement() {
        return id.getAndIncrement();
    }
}
