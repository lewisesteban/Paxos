package com.lewisesteban.paxos.paxosnode.listener;

import com.lewisesteban.paxos.paxosnode.membership.Membership;
import com.lewisesteban.paxos.paxosnode.proposer.ClientCommandContainer;
import com.lewisesteban.paxos.storage.StorageException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * All synchronization done here is for the list of unneeded instances
 */
public class UnneededInstanceGossipper {
    public static int GOSSIP_FREQUENCY = 100;

    private ClientCommandContainer clientCommandContainer;
    private Membership membership;
    private SnapshotManager snapshotManager;

    private SortedMap<Integer, GossipInstance> unneededInstanceOfNodes = new TreeMap<>();
    private Random random = new Random();
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private long lastGossipTimestamp = 0;
    private AtomicBoolean isGossipping = new AtomicBoolean(false);
    private long globalUnneededInst = -1;
    private AtomicLong gossipNumber = new AtomicLong(0);

    public UnneededInstanceGossipper(ClientCommandContainer clientCommandContainer, Membership membership,
                                     SnapshotManager snapshotManager) {
        this.clientCommandContainer = clientCommandContainer;
        this.membership = membership;
        this.snapshotManager = snapshotManager;
    }

    void sendGossipMaybe(long lastFinishedInstance) {
        if (System.currentTimeMillis() - lastGossipTimestamp > GOSSIP_FREQUENCY) {
            // update value of my own unneeded instance
            synchronized (this) {
                Long lowestNeededInst = clientCommandContainer.getLowestInstanceId();
                long myUnneededInstance;
                if (lowestNeededInst == null)
                    myUnneededInstance = lastFinishedInstance - 1;
                else
                    myUnneededInstance = lowestNeededInst - 1;
                unneededInstanceOfNodes.put(membership.getMyNodeId(), new GossipInstance(myUnneededInstance, gossipNumber.getAndIncrement()));
            }
            // send to others
            if (isGossipping.compareAndSet(false, true)) {
                lastGossipTimestamp = System.currentTimeMillis();
                executorService.submit(() -> {
                    int node1 = getRandomNodeId(membership.getMyNodeId());
                    int node2 = getRandomNodeId(node1);
                    SortedMap<Integer, GossipInstance> mapToSend;
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

    void receiveGossip(Map<Integer, GossipInstance> data) throws StorageException {
        // note: an "unneeded instance" of -1 means all instances are needed
        boolean nodeDataMissing = false;
        Long lowestReceivedUnneededInst = null;
        synchronized (this) {
            for (int nodeId = 0; nodeId < membership.getNbMembers(); nodeId++) {
                GossipInstance newVal;
                if (nodeId == membership.getMyNodeId()) {
                    newVal = unneededInstanceOfNodes.get(nodeId);
                } else {
                    GossipInstance myUnneededInst = unneededInstanceOfNodes.get(nodeId);
                    GossipInstance receivedUnneededInst = data.get(nodeId);
                    if (myUnneededInst == null)
                        newVal = receivedUnneededInst;
                    else if (receivedUnneededInst == null)
                        newVal = myUnneededInst;
                    else {
                        newVal = myUnneededInst.getGossipNumber() > receivedUnneededInst.getGossipNumber() ?
                                myUnneededInst : receivedUnneededInst;
                    }
                }
                if (newVal != null) {
                    unneededInstanceOfNodes.put(nodeId, newVal);
                    if (lowestReceivedUnneededInst == null || newVal.getInstance() < lowestReceivedUnneededInst) {
                        lowestReceivedUnneededInst = newVal.getInstance();
                    }
                } else {
                    nodeDataMissing = true;
                }
            }
        }
        if (!nodeDataMissing && lowestReceivedUnneededInst != null && lowestReceivedUnneededInst > globalUnneededInst) {
            globalUnneededInst = lowestReceivedUnneededInst;
            snapshotManager.setNewGlobalUnneededInstance(globalUnneededInst);
        }
    }
}
