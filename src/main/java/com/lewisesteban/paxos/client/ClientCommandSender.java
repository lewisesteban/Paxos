package com.lewisesteban.paxos.client;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;

import java.io.IOException;
import java.io.Serializable;

/**
 * Responsible for sending a single command to Paxos and getting its result.
 */
class ClientCommandSender {
    private long lastSentInstance = -1;
    private FailureManager failureManager;

    ClientCommandSender(FailureManager failureManager) {
        this.failureManager = failureManager;
    }

    Serializable doCommand(PaxosProposer paxosNode, Command command) throws CommandFailedException, DedicatedProposerRedirection {
        return doCommand(paxosNode, command, null);
    }

    Serializable doCommand(PaxosProposer paxosNode, Command command, Long instance) throws CommandFailedException, DedicatedProposerRedirection {
        failureManager.setOngoingCmdNb(command.getClientCmdNb());
        Serializable commandReturn = null;
        boolean success = false;
        if (instance == null)
            instance = getNewInstanceId(paxosNode, command, null);
        while (!success) {
            Result result;
            try {
                result = sendCommand(paxosNode, command, instance);
            } catch (IOException e) {
                throw new CommandFailedException(instance, command, e);
            }
            switch (result.getStatus()) {
                case Result.CONSENSUS_ON_THIS_CMD:
                    success = true;
                    commandReturn = result.getReturnData();
                    break;
                case Result.CONSENSUS_ON_ANOTHER_CMD:
                    instance = getNewInstanceId(paxosNode, command, instance);
                    break;
                case Result.NETWORK_ERROR:
                    throw new CommandFailedException(instance, command, null);
                case Result.BAD_PROPOSAL:
                    if (result.getExtra().getLeaderId() != null) {
                        throw new DedicatedProposerRedirection(result.getExtra().getLeaderId(), command, instance);
                    }
            }
        }
        return commandReturn;
    }

    private Result sendCommand(PaxosProposer paxosNode, Command command, long instance) throws IOException {
        if (failureManager != null) {
            if (instance != lastSentInstance) {
                failureManager.startInstance(instance);
                lastSentInstance = instance;
            }
        }
        return paxosNode.propose(command, instance);
    }

    private long getNewInstanceId(PaxosProposer paxosNode, Command command, Long startedInstance) throws CommandFailedException {
        try {
            return paxosNode.getNewInstanceId();
        } catch (IOException e) {
            throw new CommandFailedException(startedInstance, command, e);
        }
    }

    class CommandFailedException extends CommandException {

        CommandFailedException(Long instanceThatMayHaveBeenInitiated, Command command, Throwable cause) {
            super(instanceThatMayHaveBeenInitiated, command.getData(), command.getClientCmdNb(), cause);
        }
    }

    class DedicatedProposerRedirection extends CommandException {
        private int dedicatedProposerId;

        DedicatedProposerRedirection(int dedicatedProposerId, Command command, Long instance) {
            super(instance, command.getData(), command.getClientCmdNb(), null);
            this.dedicatedProposerId = dedicatedProposerId;
        }

        int getDedicatedProposerId() {
            return dedicatedProposerId;
        }
    }
}
