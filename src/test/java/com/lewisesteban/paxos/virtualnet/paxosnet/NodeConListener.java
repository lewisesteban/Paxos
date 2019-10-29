package com.lewisesteban.paxos.virtualnet.paxosnet;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;
import com.lewisesteban.paxos.virtualnet.VirtualConnection;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;

import java.io.IOException;

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
    public boolean execute(long instanceId, Command command) throws IOException {
        return parent.tryNetCall(() -> listenerHandle().execute(instanceId, command));
    }

    @Override
    public StateMachine.Snapshot getSnapshot() throws IOException {
        return parent.tryNetCall(() -> listenerHandle().getSnapshot());
    }

    @Override
    public long getSnapshotLastInstanceId() throws IOException {
        return parent.tryNetCall(() -> listenerHandle().getSnapshotLastInstanceId());
    }
}
