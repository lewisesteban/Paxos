package network;

import com.lewisesteban.paxos.paxosnode.membership.NodeHeartbeat;
import com.lewisesteban.paxos.rpc.paxos.MembershipRPCHandle;

import java.io.IOException;

class EmptyMembership implements MembershipRPCHandle {
    @Override
    public void gossipMemberList(NodeHeartbeat[] memberList) throws IOException {
        throw new IOException("Not connected");
    }

    @Override
    public int bullyElection(int instanceNb) throws IOException {
        throw new IOException("Not connected");
    }

    @Override
    public BullyVictoryResponse bullyVictory(int senderId, int instanceNb) throws IOException {
        throw new IOException("Not connected");
    }
}
