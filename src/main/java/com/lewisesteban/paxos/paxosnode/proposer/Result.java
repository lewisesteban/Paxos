package com.lewisesteban.paxos.paxosnode.proposer;

import java.io.Serializable;

@SuppressWarnings("WeakerAccess")
public class Result {
    private boolean success;
    private int instanceId = -1;
    private Serializable returnData = null;

    public Result(boolean success, int instanceId, Serializable returnData) {
        this.success = success;
        this.instanceId = instanceId;
        this.returnData = returnData;
    }

    public Result(boolean success, int instanceId) {
        this.success = success;
        this.instanceId = instanceId;
    }

    public Result(boolean success) {
        this.success = success;
    }

    public boolean getSuccess() {
        return success;
    }

    public int getInstanceId() {
        return instanceId;
    }

    public Serializable getReturnData() {
        return returnData;
    }
}
