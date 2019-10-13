package com.lewisesteban.paxos.client;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;

/**
 * Responsible for sending a single command to Paxos and getting its result
 */
class ClientCommandSender {

    private int cumulativeWaitTime = 0;
    private Random random = new Random();

    Serializable doCommand(PaxosProposer paxosNode, Command command) throws CommandException {
        return doCommand(paxosNode, command, null);
    }

    Serializable doCommand(PaxosProposer paxosNode, Command command, Long instance) throws CommandException {
        Serializable commandReturn = null;
        boolean success = false;
        if (instance == null)
            instance = getNewInstanceId(paxosNode, null);
        while (!success) {
            Result result;
            try {
                result = paxosNode.propose(command, instance);
            } catch (IOException e) {
                result = new Result(Result.CONSENSUS_FAILED);
            }
            switch (result.getStatus()) {
                case Result.CONSENSUS_ON_THIS_CMD:
                    success = true;
                    commandReturn = result.getReturnData();
                    break;
                case Result.CONSENSUS_ON_ANOTHER_CMD:
                    instance = getNewInstanceId(paxosNode, instance);
                    break;
                case Result.CONSENSUS_FAILED:
                    waitAfterFail();
                    break;
            }
        }
        return commandReturn;
    }

    private long getNewInstanceId(PaxosProposer paxosNode, Long startedInstance) throws CommandException {
        cumulativeWaitTime = 0;
        try {
            return paxosNode.getNewInstanceId();
        } catch (IOException e) {
            throw new CommandException(startedInstance, e);
        }
    }

    private void waitAfterFail() {
        cumulativeWaitTime += random.nextInt(20);
        try {
            Thread.sleep(cumulativeWaitTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
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
