package com.lewisesteban.paxos.paxosnode.listener;

import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.paxosnode.acceptor.Acceptor;
import com.lewisesteban.paxos.storage.StorageException;

public class SnapshotManager {
    public static int SNAPSHOT_FREQUENCY = 300;

    private final StateMachine stateMachine;
    private Listener listener;
    private Acceptor acceptor;
    private UnneededInstanceGossipper unneededInstanceGossipper;

    public SnapshotManager(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    public void setup(Listener listener, Acceptor acceptor, UnneededInstanceGossipper unneededInstanceGossipper) throws StorageException {
        this.listener = listener;
        this.acceptor = acceptor;
        this.unneededInstanceGossipper = unneededInstanceGossipper;
        if (stateMachine.getAppliedSnapshotLastInstance() > 0) {
            applySnapshotToPaxos(stateMachine.getAppliedSnapshotLastInstance(), true);
        }
    }

    void instanceFinished(long instanceId) throws StorageException {
        unneededInstanceGossipper.sendGossipMaybe(instanceId);
        if (instanceId > stateMachine.getAppliedSnapshotLastInstance() && (instanceId + 1) % SNAPSHOT_FREQUENCY == 0
                && !stateMachine.hasWaitingSnapshot()) {
            synchronized (stateMachine) {
                if (!stateMachine.hasWaitingSnapshot()) {
                    stateMachine.createWaitingSnapshot(instanceId);
                }
            }
        }
    }

    void receiveGossip(long[] gossipData) throws StorageException {
        unneededInstanceGossipper.receiveGossip(gossipData);
    }

    public void setNewGlobalUnneededInstance(long highestUnneededInstance) throws StorageException {
        if (stateMachine.hasWaitingSnapshot() && highestUnneededInstance >= stateMachine.getWaitingSnapshotLastInstance()) {
            Long appliedSnapshotLastInst = null;
            synchronized (stateMachine) {
                if (stateMachine.hasWaitingSnapshot() && highestUnneededInstance >= stateMachine.getWaitingSnapshotLastInstance()) {
                    stateMachine.applyCurrentWaitingSnapshot();
                    appliedSnapshotLastInst = stateMachine.getAppliedSnapshotLastInstance();
                }
            }
            if (appliedSnapshotLastInst != null) {
                applySnapshotToPaxos(appliedSnapshotLastInst, false);
            }
        }
    }

    public void loadSnapshot(StateMachine.Snapshot snapshot) throws StorageException {
        if (snapshot == null)
            return;
        //noinspection SynchronizeOnNonFinalField
        synchronized (listener) { // prevent execution of commands
            synchronized (stateMachine) {
                stateMachine.applySnapshot(snapshot);
            }
            applySnapshotToPaxos(snapshot.getLastIncludedInstance(), true);
        }
    }

    private void applySnapshotToPaxos(long lastSnapshotInstance, boolean loadedSnapshot) {
        try {
            acceptor.removeLogsUntil(lastSnapshotInstance);
        } catch (StorageException e) {
            e.printStackTrace();
        }
        listener.setSnapshotUpTo(lastSnapshotInstance, loadedSnapshot);
    }

    StateMachine.Snapshot getSnapshot() throws StorageException {
        synchronized (stateMachine) {
            return stateMachine.getAppliedSnapshot();
        }
    }

    public long getSnapshotLastInstance() throws StorageException {
        return stateMachine.getAppliedSnapshotLastInstance();
    }
}
