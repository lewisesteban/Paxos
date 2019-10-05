package com.lewisesteban.paxos.paxosnode.listener;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.paxosnode.MembershipGetter;
import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;

import java.util.HashSet;
import java.util.Set;

// TODO ordering
public class Listener implements ListenerRPCHandle {

    private Set<Command.Id> executedCommands = new HashSet<>();
    private MembershipGetter memberList;
    private StateMachine stateMachine;

    public Listener(MembershipGetter memberList, StateMachine stateMachine) {
        this.memberList = memberList;
        this.stateMachine = stateMachine;
    }

    @Override
    public synchronized void informConsensus(int instanceId, Command command) {
        if (!executedCommands.contains(command.getId())) {
            stateMachine.execute(command.getData());
            executedCommands.add(command.getId());
        }
    }
}
