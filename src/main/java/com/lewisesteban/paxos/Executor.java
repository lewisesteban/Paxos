package com.lewisesteban.paxos;

public interface Executor {

    void execute(int instanceId, Object o);
}
