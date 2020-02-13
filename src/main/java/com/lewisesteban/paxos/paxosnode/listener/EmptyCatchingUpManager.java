package com.lewisesteban.paxos.paxosnode.listener;

class EmptyCatchingUpManager implements CatchingUpManager {
    @Override
    public boolean isCatchingUp() {
        return true;
    }

    @Override
    public void startCatchUp(long firstInstInclusive, long lastInstInclusive) {
    }

    @Override
    public void consensusReached(long inst) {
    }
}
