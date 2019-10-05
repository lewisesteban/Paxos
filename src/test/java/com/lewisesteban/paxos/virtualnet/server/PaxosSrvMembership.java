package com.lewisesteban.paxos.virtualnet.server;

import com.lewisesteban.paxos.rpc.paxos.MembershipRPCHandle;

public class PaxosSrvMembership implements MembershipRPCHandle {

    private MembershipRPCHandle paxosMembership;
    private PaxosServer.ThreadManager threadManager;

    PaxosSrvMembership(MembershipRPCHandle paxosMembership, PaxosServer.ThreadManager threadManager) {
        this.paxosMembership = paxosMembership;
        this.threadManager = threadManager;
    }
}
