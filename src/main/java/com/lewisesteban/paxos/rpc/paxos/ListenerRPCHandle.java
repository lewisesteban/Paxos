package com.lewisesteban.paxos.rpc.paxos;

import java.io.IOException;
import java.io.Serializable;

public interface ListenerRPCHandle {

    void execute(long instanceId, Serializable command) throws IOException;
}
