package com.lewisesteban.paxos.virtualnet.server;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;

import java.io.IOException;

public class PaxosSrvListener implements ListenerRPCHandle {

    private ListenerRPCHandle paxosListener;
    private PaxosServer.ThreadManager threadManager;

    PaxosSrvListener(ListenerRPCHandle paxosListener, PaxosServer.ThreadManager threadManager) {
        this.paxosListener = paxosListener;
        this.threadManager = threadManager;
    }

    @Override
    public void execute(int instanceId, Command command) throws IOException {
        threadManager.pleaseDo(() -> {
            paxosListener.execute(instanceId, command);
            return true;
        });
    }
}
