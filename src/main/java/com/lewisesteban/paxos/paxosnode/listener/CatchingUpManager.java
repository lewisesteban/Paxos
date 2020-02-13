package com.lewisesteban.paxos.paxosnode.listener;

public interface CatchingUpManager {
    boolean isCatchingUp();
    void startCatchUp(long firstInstInclusive, long lastInstInclusive);
    void consensusReached(long inst);
}
