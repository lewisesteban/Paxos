package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;

import java.util.List;

public interface MembershipGetter {

    int getMyNodeId();
    List<RemotePaxosNode> getMembers();
    int getNbMembers();
}
