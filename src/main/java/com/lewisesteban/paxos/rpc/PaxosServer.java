package com.lewisesteban.paxos.rpc;

import java.io.Serializable;

public interface PaxosServer {

    boolean propose(Serializable proposalData);
}
