package com.lewisesteban.paxos.paxosnode.membership;

import com.lewisesteban.paxos.paxosnode.ClusterHandle;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class NodeStateSupervisor {
    public static int GOSSIP_AVG_TIME_PER_NODE = 200;
    public static int FAILURE_TIMEOUT = 1000;

    private NodeState[] nodeStates;
    private NodeHeartbeat[] nodeHeartbeats; // updated only before sending
    private ClusterHandle membership;
    private boolean running = false;
    private Random random = new Random();
    private AtomicLong heartbeatCounter = new AtomicLong(0);
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private boolean[] awaitingAnswers;

    NodeStateSupervisor(ClusterHandle membership) {
        this.membership = membership;
    }

    synchronized void start() {
        running = true;
        nodeStates = new NodeState[membership.getNbMembers()];
        awaitingAnswers = new boolean[membership.getNbMembers()];
        for (int node = 0; node < nodeStates.length; node++) {
            nodeStates[node] = new NodeState(node);
            awaitingAnswers[node] = false;
        }
        nodeHeartbeats = new NodeHeartbeat[membership.getNbMembers()];
        new Thread(this::backGroundWork).start();
    }

    void stop() {
        running = false;
    }

    synchronized void receiveMemberList(NodeHeartbeat[] memberList) {
        if (!running)
            return;
        for (int node = 0; node < nodeStates.length; ++node) {
            if (node == membership.getMyNodeId() && memberList[node] != null
                    && memberList[node].getCounter() > heartbeatCounter.get()) {
                heartbeatCounter.set(memberList[node].getCounter() + 1);
            }
            nodeStates[node].updateHeartbeat(memberList[node]);
        }
    }

    synchronized void resetTimeout(int nodeId) {
        nodeStates[nodeId].resetTimeout();
    }

    private int getTimeToWait() {
        if (membership.getNbMembers() <= 2)
            return GOSSIP_AVG_TIME_PER_NODE;
        return GOSSIP_AVG_TIME_PER_NODE / (membership.getNbMembers() - 1) * 2;
    }

    private void backGroundWork() {
        int totalWaitTime = getTimeToWait();
        while (running) {
            long start = System.currentTimeMillis();
            checkForTimeouts();
            checkForLeaderFailure();
            gossipMemberList();
            long timeToWait = totalWaitTime - (System.currentTimeMillis() - start);
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
        executorService.submit(() -> {
            if (!sendRequest(node1))
                sendRequest(getRandomNodeId(node2));
        });
        if (node2 != node1) {
            executorService.submit(() -> {
                sendRequest(node2);
            });
        }
    }

    private boolean sendRequest(int target) {
        if (target == membership.getMyNodeId())
            return false;
        if (awaitingAnswers[target])
            return false;
        awaitingAnswers[target] = true;
        try {
            membership.getMembers().get(target).getMembership().gossipMemberList(nodeHeartbeats);
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            awaitingAnswers[target] = false;
        }
    }

    private int getRandomNodeId(int exceptThisOne) {
        if (membership.getNbMembers() == 1)
            return membership.getMyNodeId();
        if (membership.getNbMembers() == 2)
            return membership.getMyNodeId() == 0 ? 1 : 0;
        int node = random.nextInt(membership.getNbMembers());
        while (node == membership.getMyNodeId() || node == exceptThisOne) {
            node = random.nextInt(membership.getNbMembers());
        }
        return node;
    }
}
