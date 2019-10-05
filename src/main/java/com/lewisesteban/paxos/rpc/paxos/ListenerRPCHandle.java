package com.lewisesteban.paxos.rpc.paxos;

import com.lewisesteban.paxos.paxosnode.Command;

import java.io.IOException;

public interface ListenerRPCHandle {

    void execute(int instanceId, Command command) throws IOException;
}
