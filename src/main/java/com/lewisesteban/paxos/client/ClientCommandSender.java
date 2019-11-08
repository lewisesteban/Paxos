package com.lewisesteban.paxos.client;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;

import java.io.IOException;
import java.io.Serializable;

/**
 * Responsible for sending a single command to Paxos and getting its result
 */
public class ClientCommandSender {

    Serializable doCommand(PaxosProposer paxosNode, Command command) throws CommandException, DedicatedProposerRedirection {
        return doCommand(paxosNode, command, null);
    }

    Serializable doCommand(PaxosProposer paxosNode, Command command, Long instance) throws CommandException, DedicatedProposerRedirection {
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
                    instance = getNewInstanceId(paxosNode, instance);
                    break;
                case Result.NETWORK_ERROR:
                    throw new CommandException(instance, null);
                case Result.BAD_PROPOSAL:
                    if (result.getExtra().getLeaderId() != null) {
                        throw new DedicatedProposerRedirection(result.getExtra().getLeaderId(), instance);
                    }
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

    private class CommandSenderExceptionBase extends Throwable {
        private Long instanceThatMayHaveBeenInitiated;

        CommandSenderExceptionBase(Long instanceThatMayHaveBeenInitiated, Throwable cause) {
            super(cause);
            this.instanceThatMayHaveBeenInitiated = instanceThatMayHaveBeenInitiated;
        }

        Long getInstanceThatMayHaveBeenInitiated() {
            return instanceThatMayHaveBeenInitiated;
        }
    }

    public class CommandException extends CommandSenderExceptionBase {

        CommandException(Long instanceThatMayHaveBeenInitiated, Throwable cause) {
            super(instanceThatMayHaveBeenInitiated, cause);
        }
    }

    class DedicatedProposerRedirection extends CommandSenderExceptionBase {
        private int dedicatedProposerId;

        DedicatedProposerRedirection(int dedicatedProposerId, Long instance) {
            super(instance, null);
            this.dedicatedProposerId = dedicatedProposerId;
        }

        int getDedicatedProposerId() {
            return dedicatedProposerId;
        }
    }
}
