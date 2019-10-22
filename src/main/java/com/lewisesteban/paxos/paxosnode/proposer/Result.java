package com.lewisesteban.paxos.paxosnode.proposer;

import java.io.Serializable;

@SuppressWarnings("WeakerAccess")
public class Result {
    public static final byte NETWORK_ERROR = 0;
    public static final byte CONSENSUS_ON_ANOTHER_CMD = 1;
    public static final byte CONSENSUS_ON_THIS_CMD = 2;

    private byte status;
    private long instanceId = -1;
    private Serializable returnData = null;

    public Result(byte status, long instanceId, Serializable returnData) {
        this.status = status;
        this.instanceId = instanceId;
        this.returnData = returnData;
    }

    public Result(byte status, long instanceId) {
        this.status = status;
        this.instanceId = instanceId;
    }

    public Result(byte status) {
        this.status = status;
    }

    public byte getStatus() {
        return status;
    }

    public long getInstanceId() {
        return instanceId;
    }

    public Serializable getReturnData() {
        return returnData;
    }
}
