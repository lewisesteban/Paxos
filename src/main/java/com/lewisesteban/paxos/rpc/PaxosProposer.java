package com.lewisesteban.paxos.rpc;

import com.lewisesteban.paxos.InstId;

import java.io.IOException;
import java.io.Serializable;

public interface PaxosProposer {

    boolean propose(InstId instanceId, Serializable proposalData) throws IOException;
}
