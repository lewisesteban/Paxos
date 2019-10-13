package com.lewisesteban.paxos.rpc.paxos;

import com.lewisesteban.paxos.paxosnode.Command;

import java.io.IOException;

public interface ListenerRPCHandle {

    void execute(long instanceId, Command command) throws IOException;
}
