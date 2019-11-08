package com.lewisesteban.paxos.rpc.paxos;

public interface RemotePaxosNode {

    /**
     * Returns the ID of the node within its fragment's cluster
     */
    int getId();

    /**
     * Returns the ID of the fragment
     */
    int getFragmentId();

    AcceptorRPCHandle getAcceptor();
    ListenerRPCHandle getListener();
    MembershipRPCHandle getMembership();
}
