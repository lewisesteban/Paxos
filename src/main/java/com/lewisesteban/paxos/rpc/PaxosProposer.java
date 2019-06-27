package com.lewisesteban.paxos.rpc;

import com.lewisesteban.paxos.Command;
import com.lewisesteban.paxos.node.proposer.Result;

import java.io.IOException;

public interface PaxosProposer {

    Result proposeNew(Command command) throws IOException;
    Result propose(Command command, int instanceId) throws IOException;
}
