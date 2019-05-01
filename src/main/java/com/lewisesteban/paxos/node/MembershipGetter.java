package com.lewisesteban.paxos.node;

import com.lewisesteban.paxos.rpc.NodeRPCHandle;

import java.util.List;

public interface MembershipGetter {

    int getMyNodeId();
    List<NodeRPCHandle> getMembers();
    int getNbMembers();
}
