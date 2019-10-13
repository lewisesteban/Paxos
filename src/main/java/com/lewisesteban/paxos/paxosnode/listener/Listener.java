package com.lewisesteban.paxos.paxosnode.listener;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.MembershipGetter;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

// TODO ordering & catching-up
public class Listener implements ListenerRPCHandle {

    private Map<Long, ExecutedCommand> executedCommands = new HashMap<>();
    private long lastInstanceId;
    private MembershipGetter memberList;
    private StateMachine stateMachine;

    public Listener(MembershipGetter memberList, StateMachine stateMachine) {
        this.memberList = memberList;
        this.stateMachine = stateMachine;
    }

    @Override
    public synchronized void execute(long instanceId, Command command) {
        if (!executedCommands.containsKey(instanceId)) {
            Serializable result = stateMachine.execute(command.getData());
            if (instanceId > lastInstanceId) {
                lastInstanceId = instanceId;
            }
            executedCommands.put(instanceId, new ExecutedCommand(command, result));
        }

    }

    /**
     * Returns the return value of a command that has been executed.
     * If that command hasn't been executed yet, it is executed and its return value is returned.
     */
    public synchronized Serializable getReturnOf(long instanceId, Command command) {
        if (!executedCommands.containsKey(instanceId)) {
            execute(instanceId, command);
        }
        return executedCommands.get(instanceId).result;
    }

    /**
     * Checks if a command has been executed in a particular instance.
     * If it has, it (the command itself) is returned.
     */
    public ExecutedCommand tryGetExecutedCommand(long instanceId) {
        if (!executedCommands.containsKey(instanceId)) {
            return null;
        }
        return executedCommands.get(instanceId);
    }

    public long getLastInstanceId() {
        return lastInstanceId;
    }

    public class ExecutedCommand {

        ExecutedCommand(Command command, Serializable result) {
            this.command = command;
            this.result = result;
        }

        Command command;
        Serializable result;

        public Command getCommand() {
            return command;
        }

        public Serializable getResult() {
            return result;
        }
    }
}
