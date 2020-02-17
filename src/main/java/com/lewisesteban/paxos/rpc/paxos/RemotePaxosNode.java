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

    /**
     * @return Non-null object to use for remotely calling an acceptor's methods
     */
    AcceptorRPCHandle getAcceptor();

    /**
     * @return Non-null object to use for remotely calling an listener's methods
     */
    ListenerRPCHandle getListener();

    /**
     * @return Non-null object to use for remotely calling methods related to membership
     */
    MembershipRPCHandle getMembership();
}
