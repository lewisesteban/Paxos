package com.lewisesteban.paxos.virtualnet.paxosnet;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;
import com.lewisesteban.paxos.virtualnet.VirtualConnection;

import java.io.IOException;

class NodeConListener implements ListenerRPCHandle {

    private VirtualConnection parent;
    private ListenerRPCHandle paxosHandle;

    NodeConListener(VirtualConnection parent, ListenerRPCHandle paxosHandle) {
        this.parent = parent;
        this.paxosHandle = paxosHandle;
    }

    @Override
    public void informConsensus(int instanceId, Command command) throws IOException {
        paxosHandle.informConsensus(instanceId, command);
    }
}
