package com.lewisesteban.paxos.paxosnode.listener;

import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.paxosnode.acceptor.Acceptor;
import com.lewisesteban.paxos.storage.StorageException;

public class SnapshotManager {
    public static int SNAPSHOT_FREQUENCY = 1000;
    public static int KEEP_AFTER_SNAPSHOT = SNAPSHOT_FREQUENCY / 4;

    private StateMachine stateMachine;
    private Listener listener;
    private Acceptor acceptor;

    public SnapshotManager(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    public void setup(Listener listener, Acceptor acceptor) {
        this.listener = listener;
        this.acceptor = acceptor;
        if (stateMachine.getSnapshotLastInstance() > 0) {
            applySnapshotToPaxos(stateMachine.getSnapshotLastInstance());
        }
    }

    void instanceFinished(long instanceId) throws StorageException {
        if (instanceId - stateMachine.getSnapshotLastInstance() >= SNAPSHOT_FREQUENCY) {
            synchronized (this) {
                stateMachine.snapshot(instanceId);
            }
            applySnapshotToPaxos(stateMachine.getSnapshotLastInstance());
        }
    }

    public void applySnapshot(StateMachine.Snapshot snapshot) {
        stateMachine.loadSnapshot(snapshot);
        applySnapshotToPaxos(snapshot.getLastIncludedInstance());
    }

    private void applySnapshotToPaxos(long lastSnapshotInstance) {
        try {
            acceptor.removeLogsUntil(lastSnapshotInstance - KEEP_AFTER_SNAPSHOT);
        } catch (StorageException e) {
            e.printStackTrace();
        }
        listener.setSnapshotUpTo(lastSnapshotInstance);
    }

    synchronized StateMachine.Snapshot getSnapshot() throws StorageException {
        return stateMachine.getSnapshot();
    }

    public long getSnapshotLastInstance() {
        return stateMachine.getSnapshotLastInstance();
    }
}
