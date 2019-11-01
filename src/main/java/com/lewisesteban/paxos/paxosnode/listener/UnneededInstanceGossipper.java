package com.lewisesteban.paxos.paxosnode.listener;

import com.lewisesteban.paxos.paxosnode.membership.Membership;
import com.lewisesteban.paxos.paxosnode.proposer.ClientCommandContainer;
import com.lewisesteban.paxos.storage.StorageException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * All synchronization done here is for the list of unneeded instances
 */
public class UnneededInstanceGossipper {
    public static int GOSSIP_FREQUENCY = 100;

    private ClientCommandContainer clientCommandContainer;
    private Membership membership;
    private SnapshotManager snapshotManager;

    private SortedMap<Integer, Long> unneededInstanceOfNodes = new TreeMap<>();
    private Random random = new Random();
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private long lastGossipTimestamp = 0;
    private AtomicBoolean isGossipping = new AtomicBoolean(false);
    private long globalUnneededInst = -1;

    public UnneededInstanceGossipper(ClientCommandContainer clientCommandContainer, Membership membership,
                                     SnapshotManager snapshotManager) {
        this.clientCommandContainer = clientCommandContainer;
        this.membership = membership;
        this.snapshotManager = snapshotManager;
    }

    void sendGossipMaybe() {
        if (System.currentTimeMillis() - lastGossipTimestamp > GOSSIP_FREQUENCY) {
            // update value of my own unneeded instance
            synchronized (this) {
                unneededInstanceOfNodes.put(membership.getMyNodeId(), clientCommandContainer.getLowestInstanceId());
            }
            // send to others
            if (isGossipping.compareAndSet(false, true)) {
                lastGossipTimestamp = System.currentTimeMillis();
                executorService.submit(() -> {
                    int node1 = getRandomNodeId(membership.getMyNodeId());
                    int node2 = getRandomNodeId(node1);
                    SortedMap<Integer, Long> mapToSend;
                    synchronized (this) {
                        mapToSend = new TreeMap<>(unneededInstanceOfNodes);
                    }
                    try {
                        membership.getMembers().get(node1).getListener().gossipUnneededInstances(mapToSend);
                    } catch (IOException ignored) { }
                    try {
                        membership.getMembers().get(node2).getListener().gossipUnneededInstances(mapToSend);
                    } catch (IOException ignored) { }
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

    void receiveGossip(Map<Integer, Long> data) throws StorageException {
        Long lowestReceivedUnneededInst = null;
        synchronized (this) {
            for (int nodeId = 0; nodeId < membership.getNbMembers(); nodeId++) {
                Long newVal = null;
                if (nodeId == membership.getMyNodeId()) {
                    newVal = unneededInstanceOfNodes.get(nodeId);
                } else {
                    Long myUnneededInst = unneededInstanceOfNodes.get(nodeId);
                    Long receivedUnneededInst = data.get(nodeId);
                    if (myUnneededInst == null && receivedUnneededInst != null)
                        newVal = receivedUnneededInst;
                    else if (myUnneededInst != null && receivedUnneededInst == null)
                        newVal = myUnneededInst;
                    else if (myUnneededInst != null)
                        newVal = myUnneededInst > receivedUnneededInst ? myUnneededInst : receivedUnneededInst;
                }
                if (newVal != null) {
                    unneededInstanceOfNodes.put(nodeId, newVal);
                    if (lowestReceivedUnneededInst == null || newVal < lowestReceivedUnneededInst)
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
