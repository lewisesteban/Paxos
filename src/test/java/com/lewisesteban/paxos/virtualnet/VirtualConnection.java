package com.lewisesteban.paxos.virtualnet;

import java.io.IOException;

/**
 * A virtual connection between two nodes: a source node and a destination node.
 */
public interface VirtualConnection {

    /**
     * This method throws if the source node cannot communicate with the destination node.
     */
    void tryNetCall() throws IOException;
}
