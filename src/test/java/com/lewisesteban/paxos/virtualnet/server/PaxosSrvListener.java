package com.lewisesteban.paxos.virtualnet.server;

import com.lewisesteban.paxos.Command;
import com.lewisesteban.paxos.rpc.ListenerRPCHandle;

import java.io.IOException;

public class PaxosSrvListener implements ListenerRPCHandle {

    private ListenerRPCHandle paxosListener;
    private PaxosServer.ThreadManager threadManager;

    PaxosSrvListener(ListenerRPCHandle paxosListener, PaxosServer.ThreadManager threadManager) {
        this.paxosListener = paxosListener;
        this.threadManager = threadManager;
    }

    @Override
    public void informConsensus(int instanceId, Command command) throws IOException {
        threadManager.pleaseDo(() -> {
            paxosListener.informConsensus(instanceId, command);
            return true;
        });
    }
}
