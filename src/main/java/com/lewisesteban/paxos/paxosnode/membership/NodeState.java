package com.lewisesteban.paxos.paxosnode.membership;

/**
 * Not synchronized
 */
class NodeState {
    private NodeHeartbeat heartbeat;
    private long lastHeartbeatTimestamp;
    private int nodeId;

    NodeState(int nodeId) {
        this.heartbeat = new NodeHeartbeat(-1);
        this.nodeId = nodeId;
        this.lastHeartbeatTimestamp = System.currentTimeMillis();
    }

    long getLastHeartbeatTimestamp() {
        return lastHeartbeatTimestamp;
    }

    int getNodeId() {
        return nodeId;
    }

    void updateHeartbeat(NodeHeartbeat receivedHeartbeat) {
        if (receivedHeartbeat != null) {
            if ((heartbeat == null && !receivedHeartbeat.isFailed())
                    || (heartbeat != null && receivedHeartbeat.getCounter() > heartbeat.getCounter())) {
                heartbeat = receivedHeartbeat;
                lastHeartbeatTimestamp = System.currentTimeMillis();
            }
        }
    }

    NodeHeartbeat getHeartbeat() {
        return heartbeat;
    }

    boolean isFailed() {
        return heartbeat == null || heartbeat.isFailed();
    }

    void nullify() {
        heartbeat = null;
    }

    boolean isNull() {
        return heartbeat == null;
    }

    void setAlive() {
        heartbeat.setFailed(false);
        lastHeartbeatTimestamp = System.currentTimeMillis();
    }

    void setFailed() {
        heartbeat.setFailed(true);
    }
}
