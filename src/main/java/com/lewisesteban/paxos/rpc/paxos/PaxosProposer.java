package com.lewisesteban.paxos.rpc.paxos;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.proposer.Result;

import java.io.IOException;

public interface PaxosProposer {

    long getNewInstanceId() throws IOException;
    Result propose(Command command, long instanceId) throws IOException;
}
