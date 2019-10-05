package com.lewisesteban.paxos.rpc.paxos;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.proposer.Result;

import java.io.IOException;

public interface PaxosProposer {

    Result proposeNew(Command command) throws IOException;
    Result propose(Command command, int instanceId) throws IOException;
}
