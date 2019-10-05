package com.lewisesteban.paxos.paxosnode.listener;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.MembershipGetter;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

// TODO ordering
public class Listener implements ListenerRPCHandle {

    private Map<Command.Id, Serializable> executedCommands = new HashMap<>();
    private MembershipGetter memberList;
    private StateMachine stateMachine;

    public Listener(MembershipGetter memberList, StateMachine stateMachine) {
        this.memberList = memberList;
        this.stateMachine = stateMachine;
    }

    @Override
    public synchronized void execute(int instanceId, Command command) {
        if (!executedCommands.containsKey(command.getId())) {
            Serializable result = stateMachine.execute(command.getData());
            executedCommands.put(command.getId(), result);
        }
    }

    public Serializable getReturnOf(int instanceId, Command command) {
        if (!executedCommands.containsKey(command.getId())) {
            execute(instanceId, command);
        }
        return executedCommands.get(command.getId());
    }
}
