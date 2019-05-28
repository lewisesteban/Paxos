package com.lewisesteban.paxos.node.listener;

import com.lewisesteban.paxos.Executor;
import com.lewisesteban.paxos.node.MembershipGetter;
import com.lewisesteban.paxos.rpc.ListenerRPCHandle;

import java.io.Serializable;

public class Listener implements ListenerRPCHandle {

    private MembershipGetter memberList;
    private Executor executor;

    public Listener(MembershipGetter memberList, Executor executor) {
        this.memberList = memberList;
        this.executor = executor;
    }

    @Override
    public void informConsensus(int instanceId, Serializable data) {
        executor.execute(instanceId, data);
    }
}
