package com.lewisesteban.paxos.rpc.paxos;

import com.lewisesteban.paxos.paxosnode.membership.NodeHeartbeat;

import java.io.IOException;

public interface MembershipRPCHandle {

    void gossipMemberList(NodeHeartbeat[] memberList) throws IOException;

    /**
     * Check whether the receiving process is eligible to be a leader.
     *
     * @return The election instance number on this node.
     */
    int bullyElection(int instanceNb) throws IOException;

    /**
     * Informs the receiving process that the sender is now the leader.
     *
     * @return Whether the victory is accepted, and the instance number of the responding node
     */
    BullyVictoryResponse bullyVictory(int senderId, int instanceNb) throws IOException;

    class BullyVictoryResponse {
        boolean victoryAccepted;
        int instanceNb;

        public BullyVictoryResponse(boolean victoryAccepted, int instanceNb) {
            this.victoryAccepted = victoryAccepted;
            this.instanceNb = instanceNb;
        }

        public boolean isVictoryAccepted() {
            return victoryAccepted;
        }

        public int getInstanceNb() {
            return instanceNb;
        }
    }
}
