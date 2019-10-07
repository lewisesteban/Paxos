package com.lewisesteban.paxos.virtualnet;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * A virtual connection between two nodes: a source node and a destination node.
 */
public interface VirtualConnection {

    /**
     * This method throws if the source node cannot communicate with the destination node.
     */
    <RT> RT tryNetCall(Callable<RT> runnable) throws IOException;
}
