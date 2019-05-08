package com.lewisesteban.paxos.virtualnet.server;

import com.lewisesteban.paxos.rpc.MembershipRPCHandle;

import java.util.concurrent.ExecutorService;

public class PaxosSrvMembership implements MembershipRPCHandle {

    private MembershipRPCHandle paxosMembership;
    private ExecutorService threadPool;

    PaxosSrvMembership(MembershipRPCHandle paxosMembership, ExecutorService threadPool) {
        this.paxosMembership = paxosMembership;
        this.threadPool = threadPool;
    }
}
