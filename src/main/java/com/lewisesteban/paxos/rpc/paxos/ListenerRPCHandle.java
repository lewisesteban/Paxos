package com.lewisesteban.paxos.rpc.paxos;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.StateMachine;

import java.io.IOException;

public interface ListenerRPCHandle {

    /**
     * Attempts to execute the specified command for the specified instance.
     * If instanceId-1 has not been executed yet, this method will wait until it has (unless instanceId is 0).
     *
     * Throws IOException if the command should be executed but hasn't because of network problems.
     * Throws StorageException if the command could not be executed because of storage-related problems
     *
     * Returns true if the command has been executed successfully.
     * Returns false if the command is contained in the snapshot and thus should not be executed again.
     */
    boolean execute(long instanceId, Command command) throws IOException;

    StateMachine.Snapshot getSnapshot() throws IOException;
    long getSnapshotLastInstanceId() throws IOException;
}
