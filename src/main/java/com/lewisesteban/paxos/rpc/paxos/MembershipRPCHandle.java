package com.lewisesteban.paxos.rpc.paxos;

import com.lewisesteban.paxos.paxosnode.membership.NodeHeartbeat;

import java.io.IOException;

public interface MembershipRPCHandle {

    void gossipMemberList(NodeHeartbeat[] memberList) throws IOException;
}
