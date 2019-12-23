package network;

import com.lewisesteban.paxos.paxosnode.membership.NodeHeartbeat;
import com.lewisesteban.paxos.rpc.paxos.MembershipRPCHandle;

import java.io.IOException;

public class PaxosMembershipClient implements RemotePaxosMembership {
    private RemoteCallManager client;
    private MembershipRPCHandle remoteMembership;

    PaxosMembershipClient(MembershipRPCHandle remoteMembership, RemoteCallManager client) {
        this.remoteMembership = remoteMembership;
        this.client = client;
    }

    @Override
    public void gossipMemberList(NodeHeartbeat[] memberList) throws IOException {
        client.doRemoteCall(() -> {
            remoteMembership.gossipMemberList(memberList);
            return true;
        });
    }

    @Override
    public int bullyElection(int instanceNb) throws IOException {
        return client.doRemoteCall(() -> remoteMembership.bullyElection(instanceNb));
    }

    @Override
    public BullyVictoryResponse bullyVictory(int senderId, int instanceNb) throws IOException {
        return client.doRemoteCall(() -> remoteMembership.bullyVictory(senderId, instanceNb));
    }
}
