package com.lewisesteban.paxos.paxosnode.membership;

import com.lewisesteban.paxos.paxosnode.ClusterHandle;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class NodeStateSupervisor {
    public static int GOSSIP_FREQUENCY = 50;
    public static int FAILURE_TIMEOUT = 1000;

    private NodeState[] nodeStates;
    private NodeHeartbeat[] nodeHeartbeats; // updated only before sending
    private ClusterHandle membership;
    private boolean keepGoing = false;
    private Random random = new Random();
    private AtomicLong heartbeatCounter = new AtomicLong(0);
    private long lastGossipTimestamp = 0;

    NodeStateSupervisor(ClusterHandle membership) {
        this.membership = membership;
    }

    void start() {
        keepGoing = true;
        nodeStates = new NodeState[membership.getNbMembers()];
        for (int node = 0; node < nodeStates.length; node++)
            nodeStates[node] = new NodeState(node);
        nodeHeartbeats = new NodeHeartbeat[membership.getNbMembers()];
        new Thread(this::backGroundWork).start();
    }

    void stop() {
        keepGoing = false;
    }

    synchronized void receiveMemberList(NodeHeartbeat[] memberList) {
        for (int node = 0; node < nodeStates.length; ++node) {
            nodeStates[node].updateHeartbeat(memberList[node]);
        }
    }

    private void backGroundWork() {
        while (keepGoing) {
            checkForTimeouts();
            checkForLeaderFailure();
            gossipMemberList();
            long timeToWait = (lastGossipTimestamp + GOSSIP_FREQUENCY) - System.currentTimeMillis();
            if (timeToWait > 0) {
                try {
                    Thread.sleep(timeToWait);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    synchronized private void checkForTimeouts() {
        for (NodeState nodeState : nodeStates) {
            if (nodeState.getNodeId() != membership.getMyNodeId() && !nodeState.isNull()) {
                if (nodeState.getHeartbeat().isFailed()) {
                    if (System.currentTimeMillis() - nodeState.getLastHeartbeatTimestamp() > FAILURE_TIMEOUT * 2) {
                        nodeState.nullify();
                    }
                } else {
                    if (System.currentTimeMillis() - nodeState.getLastHeartbeatTimestamp() > FAILURE_TIMEOUT) {
                        nodeState.setFailed();
                    }
                }
            }
        }
    }

    private void checkForLeaderFailure() {
        if (membership.getLeaderNodeId() == null || nodeStates[membership.getLeaderNodeId()].isFailed()) {
            membership.setLeaderNodeId(null);
        }
    }

    private void gossipMemberList() {
        for (int node = 0; node < nodeHeartbeats.length; node++)
            nodeHeartbeats[node] = nodeStates[node].getHeartbeat();
        nodeHeartbeats[membership.getMyNodeId()] = new NodeHeartbeat(heartbeatCounter.getAndIncrement());
        int node1 = getRandomNodeId(membership.getMyNodeId());
        int node2 = getRandomNodeId(node1);
        try {
            membership.getMembers().get(node1).getMembership().gossipMemberList(nodeHeartbeats);
        } catch (IOException ignored) { }
        try {
            membership.getMembers().get(node2).getMembership().gossipMemberList(nodeHeartbeats);
        } catch (IOException ignored) { }
        lastGossipTimestamp = System.currentTimeMillis();
    }

    private int getRandomNodeId(int exceptThisOne) {
        if (membership.getNbMembers() == 1)
            return membership.getMyNodeId();
        int node = random.nextInt(membership.getNbMembers());
        while (node == membership.getMyNodeId() || node == exceptThisOne)
            node = random.nextInt(membership.getNbMembers());
        return node;
    }
}
