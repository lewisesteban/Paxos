package com.lewisesteban.paxos.rpc;

import java.io.IOException;
import java.io.Serializable;

public interface ListenerRPCHandle {

    void informConsensus(int instanceId, Serializable data) throws IOException;
}
