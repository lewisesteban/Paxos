package com.lewisesteban.paxos.rpc.paxos;

import com.lewisesteban.paxos.paxosnode.Command;

import java.io.IOException;

public interface ListenerRPCHandle {

    /**
     * Attempts to execute the specified command for the specified instance.
     * If instanceId-1 has not been executed yet, this method will wait until it has (unless instanceId is 0).
     * Returns whether or not the command was successfully executed.
     */
    boolean execute(long instanceId, Command command) throws IOException;
}
