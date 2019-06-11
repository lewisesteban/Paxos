package com.lewisesteban.paxos.virtualnet.paxosnet;

import com.lewisesteban.paxos.rpc.ListenerRPCHandle;
import com.lewisesteban.paxos.virtualnet.VirtualConnection;

import java.io.IOException;
import java.io.Serializable;

class NodeConListener implements ListenerRPCHandle {

    private VirtualConnection parent;
    private ListenerRPCHandle paxosHandle;

    NodeConListener(VirtualConnection parent, ListenerRPCHandle paxosHandle) {
        this.parent = parent;
        this.paxosHandle = paxosHandle;
    }

    @Override
    public void informConsensus(int instanceId, Serializable data) throws IOException {
        paxosHandle.informConsensus(instanceId, data);
    }
}
