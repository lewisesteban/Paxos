package com.lewisesteban.paxos.virtualnet.node;

import com.lewisesteban.paxos.rpc.ListenerRPCHandle;
import com.lewisesteban.paxos.virtualnet.VirtualConnection;

class NodeConListener implements ListenerRPCHandle {

    private VirtualConnection parent;
    private ListenerRPCHandle paxosHandle;

    NodeConListener(VirtualConnection parent, ListenerRPCHandle paxosHandle) {
        this.parent = parent;
        this.paxosHandle = paxosHandle;
    }
}
