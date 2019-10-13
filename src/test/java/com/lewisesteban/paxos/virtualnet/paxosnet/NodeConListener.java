package com.lewisesteban.paxos.virtualnet.paxosnet;

import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;
import com.lewisesteban.paxos.virtualnet.VirtualConnection;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;

import java.io.IOException;
import java.io.Serializable;

class NodeConListener implements ListenerRPCHandle {

    private VirtualConnection parent;
    private PaxosServer paxosHandle;

    NodeConListener(VirtualConnection parent, PaxosServer paxosHandle) {
        this.parent = parent;
        this.paxosHandle = paxosHandle;
    }

    private ListenerRPCHandle listenerHandle() {
        return paxosHandle.getListener();
    }

    @Override
    public void execute(long instanceId, Serializable command) throws IOException {
        parent.tryNetCall(() -> {
            listenerHandle().execute(instanceId, command);
            return true;
        });
    }
}
