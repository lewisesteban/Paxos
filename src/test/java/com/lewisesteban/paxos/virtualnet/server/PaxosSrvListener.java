package com.lewisesteban.paxos.virtualnet.server;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;

import java.io.IOException;
import java.util.Map;

public class PaxosSrvListener implements ListenerRPCHandle {

    private ListenerRPCHandle paxosListener;
    private PaxosServer.ThreadManager threadManager;

    PaxosSrvListener(ListenerRPCHandle paxosListener, PaxosServer.ThreadManager threadManager) {
        this.paxosListener = paxosListener;
        this.threadManager = threadManager;
    }

    @Override
    public boolean execute(long instanceId, Command command) throws IOException {
        return threadManager.pleaseDo(() -> paxosListener.execute(instanceId, command));
    }

    @Override
    public StateMachine.Snapshot getSnapshot() throws IOException {
        return threadManager.pleaseDo(() -> paxosListener.getSnapshot());
    }

    @Override
    public long getSnapshotLastInstanceId() throws IOException {
        return threadManager.pleaseDo(() -> paxosListener.getSnapshotLastInstanceId());
    }

    @Override
    public void gossipUnneededInstances(Map<Integer, Long> unneededInstancesOfNodes) throws IOException {
        threadManager.pleaseDo(() -> {
            paxosListener.gossipUnneededInstances(unneededInstancesOfNodes);
            return true;
        });
    }
}
