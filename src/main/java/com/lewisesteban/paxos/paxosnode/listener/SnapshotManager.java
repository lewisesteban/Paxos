package com.lewisesteban.paxos.paxosnode.listener;

import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.paxosnode.acceptor.Acceptor;
import com.lewisesteban.paxos.storage.StorageException;

import java.util.Map;

/**
 * All synchronization done here is for the state machine's snapshot
 */
public class SnapshotManager {
    public static int SNAPSHOT_FREQUENCY = 1000;

    private StateMachine stateMachine;
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
            applySnapshotToPaxos(stateMachine.getAppliedSnapshotLastInstance());
        }
    }

    void instanceFinished(long instanceId) throws StorageException {
        unneededInstanceGossipper.sendGossipMaybe();
        if (instanceId - stateMachine.getAppliedSnapshotLastInstance() >= SNAPSHOT_FREQUENCY
                && !stateMachine.hasWaitingSnapshot()) {
            synchronized (this) {
                if (!stateMachine.hasWaitingSnapshot()) {
                    stateMachine.createWaitingSnapshot(instanceId);
                }
            }
        }
    }

    void receiveGossip(Map<Integer, Long> gossipData) throws StorageException {
        unneededInstanceGossipper.receiveGossip(gossipData);
    }

    void setNewGlobalUnneededInstance(long highestUnneededInstance) throws StorageException {
        if (stateMachine.hasWaitingSnapshot()) {
            Long appliedSnapshotLastInst = null;
            synchronized (this) {
                if (stateMachine.hasWaitingSnapshot()
                        && highestUnneededInstance > stateMachine.getWaitingSnapshotLastInstance()) {
                    stateMachine.applyCurrentWaitingSnapshot();
                    appliedSnapshotLastInst = stateMachine.getAppliedSnapshotLastInstance();
                }
            }
            if (appliedSnapshotLastInst != null) {
                applySnapshotToPaxos(appliedSnapshotLastInst);
            }
        }
    }

    public void loadSnapshot(StateMachine.Snapshot snapshot) throws StorageException {
        synchronized (this) {
            stateMachine.applySnapshot(snapshot);
        }
        applySnapshotToPaxos(snapshot.getLastIncludedInstance());
    }

    private void applySnapshotToPaxos(long lastSnapshotInstance) {
        try {
            acceptor.removeLogsUntil(lastSnapshotInstance);
        } catch (StorageException e) {
            e.printStackTrace();
        }
        listener.setSnapshotUpTo(lastSnapshotInstance);
    }

    synchronized StateMachine.Snapshot getSnapshot() throws StorageException {
        return stateMachine.getAppliedSnapshot();
    }

    public long getSnapshotLastInstance() throws StorageException {
        return stateMachine.getAppliedSnapshotLastInstance();
    }
}
