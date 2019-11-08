package com.lewisesteban.paxos.paxosnode.proposer;

import java.io.Serializable;

@SuppressWarnings("WeakerAccess")
public class Result {
    /**
     * The proposal may or may not have been started and/or finished, but one or more network error(s) are preventing
     * its confirmation.
     */
    public static final byte NETWORK_ERROR = 0;
    /**
     * Consensus has already been reached on this instance, but for another command.
     * Please try again with another (higher) instance.
     */
    public static final byte CONSENSUS_ON_ANOTHER_CMD = 1;
    /**
     * Success: consensus has been reached for the specified command on the specified instance.
     */
    public static final byte CONSENSUS_ON_THIS_CMD = 2;
    /**
     * The proposal has not been started.
     * Please check the "extraData" variable and make the required modifications.
     */
    public static final byte BAD_PROPOSAL = 4;

    private byte status;
    private long instanceId = -1;
    private Serializable returnData = null;
    private ExtraData extra = null;

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

    public Result(long instanceId, int leaderId) {
        this.status = BAD_PROPOSAL;
        this.instanceId = instanceId;
        this.extra = new ExtraData(leaderId);
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

    public ExtraData getExtra() {
        return extra;
    }

    public class ExtraData {
        Integer leaderId = null;

        ExtraData(int leaderId) {
            this.leaderId = leaderId;
        }

        public Integer getLeaderId() {
            return leaderId;
        }
    }
}
