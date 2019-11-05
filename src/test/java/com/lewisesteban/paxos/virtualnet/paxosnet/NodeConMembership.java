package com.lewisesteban.paxos.virtualnet.paxosnet;

import com.lewisesteban.paxos.paxosnode.membership.NodeHeartbeat;
import com.lewisesteban.paxos.rpc.paxos.MembershipRPCHandle;
import com.lewisesteban.paxos.virtualnet.VirtualConnection;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;

import java.io.IOException;

class NodeConMembership implements MembershipRPCHandle {

    private VirtualConnection parent;
    private PaxosServer paxosHandle;

    NodeConMembership(VirtualConnection parent, PaxosServer paxosHandle) {
        this.parent = parent;
        this.paxosHandle = paxosHandle;
    }

    private MembershipRPCHandle membershipHandle() {
        return paxosHandle.getMembership();
    }

    @Override
    public void gossipMemberList(NodeHeartbeat[] memberList) throws IOException {
        parent.tryNetCall(() -> {
            membershipHandle().gossipMemberList(memberList);
            return true;
        });
    }
}
