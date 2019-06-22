package com.lewisesteban.paxos.node.listener;

import com.lewisesteban.paxos.Executor;
import com.lewisesteban.paxos.node.MembershipGetter;
import com.lewisesteban.paxos.rpc.ListenerRPCHandle;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Listener implements ListenerRPCHandle {

    private Set<Integer> executedInstances = new HashSet<>();
    private MembershipGetter memberList;
    private Executor executor;

    public Listener(MembershipGetter memberList, Executor executor) {
        this.memberList = memberList;
        this.executor = executor;
    }

    @Override
    public synchronized void informConsensus(int instanceId, Serializable data) {
        if (!executedInstances.contains(instanceId)) {
            executor.execute(instanceId, data);
            executedInstances.add(instanceId);
        }
    }

    //TODO there is a scenario in which a command may be executed twice:
    // proposer 1 sends command A, it is prepared and sent to a single acceptor, which accepts it
    // proposer 2 sends command B in the same instance as proposer 1
    // proposer 2's command arrives at the acceptor, which informs proposer 2 of the command already accepted
    // proposer 2 changes the data of its command to make it identical to command A
    // proposer 2 sends this command to all acceptors and then all listeners
    // proposer 1 sends command A to other acceptor, which refuse because they have already accepted command B
    // proposer 1 fails and its client decides to retry, thus creating a new command C with the same data
    // command C gets executed at every listener
}
