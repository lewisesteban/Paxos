package com.lewisesteban.paxos.virtualnet.paxosnet;

import com.lewisesteban.paxos.rpc.MembershipRPCHandle;
import com.lewisesteban.paxos.virtualnet.VirtualConnection;

class NodeConMembership implements MembershipRPCHandle {

    private VirtualConnection parent;
    private MembershipRPCHandle paxosHandle;

    NodeConMembership(VirtualConnection parent, MembershipRPCHandle paxosHandle) {
        this.parent = parent;
        this.paxosHandle = paxosHandle;
    }
}
