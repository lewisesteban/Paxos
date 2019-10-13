package com.lewisesteban.paxos.rpc.paxos;

import com.lewisesteban.paxos.paxosnode.proposer.Result;

import java.io.IOException;
import java.io.Serializable;

public interface PaxosProposer {

    long getNewInstanceId() throws IOException;
    Result propose(Serializable command, long instanceId) throws IOException;
}
