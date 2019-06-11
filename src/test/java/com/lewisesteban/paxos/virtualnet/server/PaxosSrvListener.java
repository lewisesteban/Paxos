package com.lewisesteban.paxos.virtualnet.server;

import com.lewisesteban.paxos.rpc.ListenerRPCHandle;

import java.io.IOException;
import java.io.Serializable;

public class PaxosSrvListener implements ListenerRPCHandle {

    private ListenerRPCHandle paxosListener;
    private PaxosServer.ThreadManager threadManager;

    PaxosSrvListener(ListenerRPCHandle paxosListener, PaxosServer.ThreadManager threadManager) {
        this.paxosListener = paxosListener;
        this.threadManager = threadManager;
    }

    @Override
    public void informConsensus(int instanceId, Serializable data) throws IOException {
        threadManager.pleaseDo(() -> {
            paxosListener.informConsensus(instanceId, data);
            return true;
        });
    }
}
