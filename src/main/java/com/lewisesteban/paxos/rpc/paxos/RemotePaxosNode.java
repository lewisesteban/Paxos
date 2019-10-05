package com.lewisesteban.paxos.rpc.paxos;

public interface RemotePaxosNode {

    int getId();
    AcceptorRPCHandle getAcceptor();
    ListenerRPCHandle getListener();
    MembershipRPCHandle getMembership();
}
