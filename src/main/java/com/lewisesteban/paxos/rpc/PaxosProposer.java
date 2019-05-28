package com.lewisesteban.paxos.rpc;

import com.lewisesteban.paxos.node.proposer.Result;

import java.io.IOException;
import java.io.Serializable;

public interface PaxosProposer {

    Result proposeNew(Serializable proposalData) throws IOException;
    Result propose(Serializable proposalData, int instanceId) throws IOException;
}
