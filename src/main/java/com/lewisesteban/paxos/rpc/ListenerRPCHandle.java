package com.lewisesteban.paxos.rpc;

import com.lewisesteban.paxos.Command;

import java.io.IOException;

public interface ListenerRPCHandle {

    void informConsensus(int instanceId, Command command) throws IOException;
}
