package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;

import java.util.List;

public interface MembershipGetter {

    /**
     * @return Unique identifier of the calling node.
     * This ID is the index of the current node in the list of nodes.
     */
    int getMyNodeId();

    List<RemotePaxosNode> getMembers();

    int getNbMembers();

    RemotePaxosNode getMyNode();
}
