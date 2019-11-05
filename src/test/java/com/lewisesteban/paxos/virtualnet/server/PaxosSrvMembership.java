package com.lewisesteban.paxos.virtualnet.server;

import com.lewisesteban.paxos.paxosnode.membership.NodeHeartbeat;
import com.lewisesteban.paxos.rpc.paxos.MembershipRPCHandle;

import java.io.IOException;

public class PaxosSrvMembership implements MembershipRPCHandle {

    private MembershipRPCHandle paxosMembership;
    private PaxosServer.ThreadManager threadManager;

    PaxosSrvMembership(MembershipRPCHandle paxosMembership, PaxosServer.ThreadManager threadManager) {
        this.paxosMembership = paxosMembership;
        this.threadManager = threadManager;
    }

    @Override
    public void gossipMemberList(NodeHeartbeat[] memberList) throws IOException {
        threadManager.pleaseDo(() -> {
            paxosMembership.gossipMemberList(memberList);
            return true;
        });
    }
}
