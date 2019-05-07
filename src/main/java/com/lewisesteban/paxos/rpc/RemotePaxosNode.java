package com.lewisesteban.paxos.rpc;

public interface RemotePaxosNode {

    int getId();
    AcceptorRPCHandle getAcceptor();
    ListenerRPCHandle getListener();
    MembershipRPCHandle getMembership();
}
