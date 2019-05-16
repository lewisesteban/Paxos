package com.lewisesteban.paxos.rpc;

import java.io.IOException;
import java.io.Serializable;

public interface PaxosProposer {

    boolean propose(long instanceId, Serializable proposalData) throws IOException;
}
