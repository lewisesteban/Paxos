package com.lewisesteban.paxos.paxosnode.listener;

import com.lewisesteban.paxos.paxosnode.membership.Membership;
import com.lewisesteban.paxos.paxosnode.proposer.ClientCommandContainer;
import com.lewisesteban.paxos.storage.StorageException;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class UnneededInstanceGossipper {
    public static int GOSSIP_FREQUENCY = 100;

    private ClientCommandContainer clientCommandContainer;
    private Membership membership;
    private SnapshotManager snapshotManager;

    private long[] unneededInstanceOfNodes; // we use the nodeId as the position in the array
    private Random random = new Random();
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private long lastGossipTimestamp = 0;
    private AtomicBoolean isGossipping = new AtomicBoolean(false);
    private long globalUnneededInst = -1;

    public UnneededInstanceGossipper(ClientCommandContainer clientCommandContainer, SnapshotManager snapshotManager) {
        this.clientCommandContainer = clientCommandContainer;
        this.snapshotManager = snapshotManager;
    }

    public void setup(Membership membership) {
        this.membership = membership;
        unneededInstanceOfNodes = new long[membership.getNbMembers()];
        for (int i = 0; i < unneededInstanceOfNodes.length; ++i) {
            unneededInstanceOfNodes[i] = -1;
        }
    }

    void sendGossipMaybe(long lastFinishedInstance) {
        if (System.currentTimeMillis() - lastGossipTimestamp > GOSSIP_FREQUENCY) {
            // update value of my own unneeded instance
            Long lowestNeededInst = clientCommandContainer.getLowestInstanceId();
            long myUnneededInstance;
            if (lowestNeededInst == null)
                myUnneededInstance = lastFinishedInstance - 1;
            else
                myUnneededInstance = lowestNeededInst - 1;
            unneededInstanceOfNodes[membership.getMyNodeId()] = myUnneededInstance;
            // send to others
            if (isGossipping.compareAndSet(false, true)) {
                lastGossipTimestamp = System.currentTimeMillis();
                executorService.submit(() -> {
                    int node1 = getRandomNodeId(membership.getMyNodeId());
                    int node2 = getRandomNodeId(node1);
                    try {
                        membership.getMembers().get(node1).getListener().gossipUnneededInstances(unneededInstanceOfNodes);
                    } catch (IOException ignored) {
                    }
                    try {
                        membership.getMembers().get(node2).getListener().gossipUnneededInstances(unneededInstanceOfNodes);
                    } catch (IOException ignored) {
                    }
                    isGossipping.set(false);
                });
            }
        }
    }

    private int getRandomNodeId(int exceptThisOne) {
        if (membership.getNbMembers() == 1)
            return membership.getMyNodeId();
        int node = random.nextInt(membership.getNbMembers());
        while (node == membership.getMyNodeId() || node == exceptThisOne)
            node = random.nextInt(membership.getNbMembers());
        return node;
    }

    void receiveGossip(long[] data) throws StorageException {
        // note: an "unneeded instance" of -1 means all instances are needed
        Long lowestReceivedUnneededInst = null;
        synchronized (this) {
            for (int nodeId = 0; nodeId < membership.getNbMembers(); nodeId++) {
                Long newVal;
                if (nodeId == membership.getMyNodeId()) {
                    newVal = unneededInstanceOfNodes[nodeId];
                } else {
                    long myUnneededInst = unneededInstanceOfNodes[nodeId];
                    long receivedUnneededInst = data[nodeId];
                    newVal = myUnneededInst > receivedUnneededInst ? myUnneededInst : receivedUnneededInst;
                }
                unneededInstanceOfNodes[nodeId] = newVal;
                if (lowestReceivedUnneededInst == null || newVal < lowestReceivedUnneededInst) {
                    lowestReceivedUnneededInst = newVal;
                }
            }
        }
        if (lowestReceivedUnneededInst != null && lowestReceivedUnneededInst > globalUnneededInst) {
            globalUnneededInst = lowestReceivedUnneededInst;
            snapshotManager.setNewGlobalUnneededInstance(globalUnneededInst);
        }
    }
}
