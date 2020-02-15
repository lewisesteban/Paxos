package com.lewisesteban.paxos.client;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;

import java.io.IOException;
import java.io.Serializable;

/**
 * Responsible for sending a single command to a single Paxos and getting its result.
 */
public class ClientCommandSender {
    private long lastSentInstance = -1;
    private FailureManager failureManager;

    public ClientCommandSender(FailureManager failureManager) {
        this.failureManager = failureManager;
    }

    public Serializable doCommand(PaxosProposer paxosNode, Command command) throws CommandFailedException, DedicatedProposerRedirection {
        return doCommand(paxosNode, command, null);
    }

    Serializable doCommand(PaxosProposer paxosNode, Command command, Long instance) throws CommandFailedException, DedicatedProposerRedirection {
        if (failureManager != null)
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
            }
            if (result.getExtra() != null && result.getExtra().getLeaderId() != null) {
                throw new DedicatedProposerRedirection(result.getExtra().getLeaderId(), command, instance,
                        result.getStatus() == Result.CONSENSUS_ON_THIS_CMD, result.getReturnData());
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
        private Serializable cmdResult;
        private boolean success;

        DedicatedProposerRedirection(int dedicatedProposerId, Command command, Long instance, boolean success, Serializable result) {
            super(instance, command.getData(), command.getClientCmdNb(), null);
            this.dedicatedProposerId = dedicatedProposerId;
            this.success = success;
            this.cmdResult = result;
        }

        int getDedicatedProposerId() {
            return dedicatedProposerId;
        }

        Serializable getCmdResult() {
            return cmdResult;
        }

        boolean isSuccess() {
            return success;
        }
    }
}
