package com.lewisesteban.paxos.rpc.paxos;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.proposer.Result;

import java.io.IOException;

public interface PaxosProposer {

    /**
     * Returns a new instance that should be available for proposing a command.
     */
    long getNewInstanceId() throws IOException;

    /**
     * Try to execute a command on the specified instance.
     * This instance must be returned from getNewInstanceId.
     */
    Result propose(Command command, long instanceId) throws IOException;

    /**
     * Signals Paxos that this client is not going to send any more requests for the moment.
     * Data related to previously-sent request may be lost.
     * Always call this before stopping a client.
     */
    void endClient(String clientId) throws IOException;
}
