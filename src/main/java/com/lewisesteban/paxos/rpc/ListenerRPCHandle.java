package com.lewisesteban.paxos.rpc;

import java.io.Serializable;

public interface ListenerRPCHandle {

    void informConsensus(int instanceId, Serializable data);
}
