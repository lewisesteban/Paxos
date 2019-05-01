package com.lewisesteban.paxos.rpc;

public interface NodeRPCHandle {

    int getId();
    AcceptorRPCHandle getAcceptor();
    ListenerRPCHandle getListener();
    MembershipRPCHandle getMembership();
}
