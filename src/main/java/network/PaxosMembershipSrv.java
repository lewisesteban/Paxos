package network;

import com.lewisesteban.paxos.paxosnode.membership.NodeHeartbeat;
import com.lewisesteban.paxos.rpc.paxos.MembershipRPCHandle;

import java.io.IOException;

public class PaxosMembershipSrv implements RemotePaxosMembership {
    private MembershipRPCHandle paxosMembership;

    PaxosMembershipSrv(MembershipRPCHandle paxosMembership) {
        this.paxosMembership = paxosMembership;
    }

    @Override
    public void gossipMemberList(NodeHeartbeat[] memberList) throws IOException {
        paxosMembership.gossipMemberList(memberList);
    }

    @Override
    public int bullyElection(int instanceNb) throws IOException {
        return paxosMembership.bullyElection(instanceNb);
    }

    @Override
    public BullyVictoryResponse bullyVictory(int senderId, int instanceNb) throws IOException {
        return paxosMembership.bullyVictory(senderId, instanceNb);
    }
}
