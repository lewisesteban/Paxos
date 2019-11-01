package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.storage.StorageException;

import java.io.Serializable;

/**
 * Does not need to handle thread synchronization, as all calls to it will be synchronized already.
 */
public interface StateMachine {

    void setNodeId(int nodeId);

    /**
     * Execute de command on the state machine
     */
    Serializable execute(Serializable data);

    /**
     * Creates a snapshot (copy of the state machine), and keeps it in memory for later.
     * The idOfLastExecutedInstance is the last instance of Paxos that has been executed on the state machine before
     * this method was called. In other words, it is the last instance included within the snapshot that will be creatd.
     * It should be kept along with the snapshot.
     */
    void createWaitingSnapshot(long idOfLastExecutedInstance);

    /**
     * Returns the waiting snapshot created by "createWaitingSnapshot"
     */
    Snapshot getWaitingSnapshot();

    /**
     * Returns the last applied snapshot (see "applySnapshot")
     */
    Snapshot getAppliedSnapshot() throws StorageException;

    /**
     * Returns the last instance included within the waiting snapshot.
     */
    long getWaitingSnapshotLastInstance();

    /**
     * Returns the last instance included within the applied snapshot.
     */
    long getAppliedSnapshotLastInstance() throws StorageException;

    /**
     * Sets the current "waiting snapshot" to be the "applied snapshot".
     * The state machine then has no more waiting snapshot.
     * The snapshot must be saved into stable storage before this method returns.
     * When the state machine starts, the applied snapshot must be loaded, and the state of the state machine must be
     * set accordingly.
     */
    void applyCurrentWaitingSnapshot() throws StorageException;

    /**
     * Similar to "applyCurrentWaitingSnapshot", except the snapshot to be applied is the one given as parameter,
     * and the whole state of the state machine must be set to that of the snapshot.
     */
    void applySnapshot(Snapshot snapshot) throws StorageException;

    /**
     * Whether or not there is a waiting snapshot.
     */
    boolean hasWaitingSnapshot();

    /**
     * Whether or not there is an applied snapshot.
     */
    boolean hasAppliedSnapshot() throws StorageException;

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
