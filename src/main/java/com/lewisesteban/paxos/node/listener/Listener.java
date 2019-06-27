package com.lewisesteban.paxos.node.listener;

import com.lewisesteban.paxos.Command;
import com.lewisesteban.paxos.Executor;
import com.lewisesteban.paxos.node.MembershipGetter;
import com.lewisesteban.paxos.rpc.ListenerRPCHandle;

import java.util.HashSet;
import java.util.Set;

public class Listener implements ListenerRPCHandle {

    private Set<Command.Id> executedCommands = new HashSet<>();
    private MembershipGetter memberList;
    private Executor executor;

    public Listener(MembershipGetter memberList, Executor executor) {
        this.memberList = memberList;
        this.executor = executor;
    }

    @Override
    public synchronized void informConsensus(int instanceId, Command command) {
        if (!executedCommands.contains(command.getId())) {
            executor.execute(instanceId, command);
            executedCommands.add(command.getId());
        }
    }
}
