package com.lewisesteban.paxos.node.proposer;

public class Result {
    private boolean success;
    private int instanceId;

    public Result(boolean success, int instanceId) {
        this.success = success;
        this.instanceId = instanceId;
    }

    public Result(boolean success) {
        this.success = success;
        this.instanceId = -1;
    }

    public boolean getSuccess() {
        return success;
    }

    public int getInstanceId() {
        return instanceId;
    }
}
