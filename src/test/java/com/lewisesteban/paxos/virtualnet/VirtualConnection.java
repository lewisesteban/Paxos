package com.lewisesteban.paxos.virtualnet;

import java.io.IOException;

public interface VirtualConnection {

    void tryNetCall() throws IOException;
}
