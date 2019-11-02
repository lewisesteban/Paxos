package com.lewisesteban.paxos.paxosnode.proposer;

import com.lewisesteban.paxos.paxosnode.MembershipGetter;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.paxosnode.listener.SnapshotManager;
import sun.awt.Mutex;

import java.io.IOException;
import java.util.Random;

class SnapshotRequester {
    private Mutex mutex = new Mutex();
    private SnapshotManager snapshotManager;
    private MembershipGetter membership;

    SnapshotRequester(SnapshotManager snapshotManager, MembershipGetter membership) {
        this.snapshotManager = snapshotManager;
        this.membership = membership;

    }

    void requestSnapshot(long instanceId) throws IOException {
        mutex.lock();
        try {
            // make sure snapshot hasn't been received yet
            if (snapshotManager.getSnapshotLastInstance() == -1 || instanceId > snapshotManager.getSnapshotLastInstance()) {
                System.out.println("node=" + membership.getMyNodeId() + " perform snapshot request inst=" + instanceId);
                int chosenNode = chooseNodeToDownloadSnapshotFrom(instanceId);
                StateMachine.Snapshot snapshot = membership.getMembers().get(chosenNode).getListener().getSnapshot();
                snapshotManager.loadSnapshot(snapshot);
            }
        } finally {
            mutex.unlock();
        }
    }

    private int chooseNodeToDownloadSnapshotFrom(long instanceForWhichSnapshotIsRequired) throws IOException {
        Random random = new Random();
        int chosenNode = random.nextInt(membership.getNbMembers());
        int iterations = 0;
        while (chosenNode == membership.getMyNodeId() || membership.getMembers().get(chosenNode).getListener().getSnapshotLastInstanceId() < instanceForWhichSnapshotIsRequired) {
            chosenNode++;
            if (chosenNode >= membership.getNbMembers())
                chosenNode = 0;
            iterations++;
            if (iterations >= membership.getNbMembers())
                throw new IOException("impossible to request snapshot");
        }
        return chosenNode;
    }
}
