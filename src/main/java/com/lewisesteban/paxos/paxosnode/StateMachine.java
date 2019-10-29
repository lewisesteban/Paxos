package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.storage.StorageException;

import java.io.Serializable;

/**
 * Does not need to handle thread synchronization, as all calls to it will be synchronized already.
 */
public interface StateMachine {

    void setNodeId(int nodeId);
    Serializable execute(Serializable data);
    void snapshot(long idOfLastExecutedInstance) throws StorageException;
    Snapshot getSnapshot() throws StorageException;
    long getSnapshotLastInstance();
    void loadSnapshot(Snapshot snapshot);

    class Snapshot implements Serializable {
        private long lastIncludedInstance;
        private Serializable data;

        public Snapshot(long lastIncludedInstance, Serializable data) {
            this.lastIncludedInstance = lastIncludedInstance;
            this.data = data;
        }

        public long getLastIncludedInstance() {
            return lastIncludedInstance;
        }

        public Serializable getData() {
            return data;
        }
    }
}
