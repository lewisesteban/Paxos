package com.lewisesteban.paxos.client;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;

public class BasicPaxosClient {

    private PaxosProposer paxosNode;
    private int cumulativeWaitTime = 0;
    private Random random = new Random();
    private Command.Factory commandFactory;

    public BasicPaxosClient(PaxosProposer paxosNode, String clientId) {
        this.paxosNode = paxosNode;
        this.commandFactory = new Command.Factory(clientId);
    }

    public Serializable doCommand(Serializable commandData) throws IOException {
        Command command = commandFactory.make(commandData);
        Serializable commandReturn = null;
        boolean success = false;
        long instance = getNewInstanceId();
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
                    instance = getNewInstanceId();
                    break;
                case Result.CONSENSUS_FAILED:
                    waitAfterFail();
                    break;
            }
        }
        return commandReturn;
    }

    private long getNewInstanceId() throws IOException {
        cumulativeWaitTime = 0;
        return paxosNode.getNewInstanceId();
    }

    private void waitAfterFail() {
        cumulativeWaitTime += random.nextInt(20);
        try {
            Thread.sleep(cumulativeWaitTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
