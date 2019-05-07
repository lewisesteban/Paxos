package com.lewisesteban.paxos.node;

import com.lewisesteban.paxos.rpc.RemotePaxosNode;

import java.util.List;

public interface MembershipGetter {

    int getMyNodeId();
    List<RemotePaxosNode> getMembers();
    int getNbMembers();
}
