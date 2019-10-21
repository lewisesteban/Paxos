package com.lewisesteban.paxos.client;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;

import java.io.IOException;
import java.io.Serializable;

/**
 * Responsible for sending a single command to Paxos and getting its result
 */
class ClientCommandSender {

    Serializable doCommand(PaxosProposer paxosNode, Command command) throws CommandException {
        return doCommand(paxosNode, command, null);
    }

    private Serializable doCommand(PaxosProposer paxosNode, Command command, Long instance) throws CommandException {
        Serializable commandReturn = null;
        boolean success = false;
        if (instance == null)
            instance = getNewInstanceId(paxosNode, null);
        while (!success) {
            Result result;
            try {
                result = paxosNode.propose(command, instance);
            } catch (IOException e) {
                throw new CommandException(instance, e);
            }
            switch (result.getStatus()) {
                case Result.CONSENSUS_ON_THIS_CMD:
                    success = true;
                    commandReturn = result.getReturnData();
                    break;
                case Result.CONSENSUS_ON_ANOTHER_CMD:
                case Result.INSTANCE_ALREADY_RUNNING:
                    instance = getNewInstanceId(paxosNode, instance);
                    break;
                case Result.NETWORK_ERROR:
                    break; // for now, just try again on the same server
            }
        }
        return commandReturn;
    }

    private long getNewInstanceId(PaxosProposer paxosNode, Long startedInstance) throws CommandException {
        try {
            return paxosNode.getNewInstanceId();
        } catch (IOException e) {
            throw new CommandException(startedInstance, e);
        }
    }

    class CommandException extends IOException {
        Long instanceThatMayHaveBeenInitiated;

        CommandException(Long instanceThatMayHaveBeenInitiated, Throwable cause) {
            super(cause);
            this.instanceThatMayHaveBeenInitiated = instanceThatMayHaveBeenInitiated;
        }
    }
}
