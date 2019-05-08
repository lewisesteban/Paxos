package com.lewisesteban.paxos.virtualnet.server;

import com.lewisesteban.paxos.rpc.ListenerRPCHandle;

import java.util.concurrent.ExecutorService;

public class PaxosSrvListener implements ListenerRPCHandle {

    private ListenerRPCHandle paxosListener;
    private ExecutorService threadPool;

    PaxosSrvListener(ListenerRPCHandle paxosListener, ExecutorService threadPool) {
        this.paxosListener = paxosListener;
        this.threadPool = threadPool;
    }
}
