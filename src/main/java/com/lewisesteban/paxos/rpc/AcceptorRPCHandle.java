package com.lewisesteban.paxos.rpc;

import com.lewisesteban.paxos.InstId;
import com.lewisesteban.paxos.node.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.node.proposer.Proposal;

import java.io.IOException;

public interface AcceptorRPCHandle {

    PrepareAnswer reqPrepare(InstId instanceId, Proposal.ID propId) throws IOException;
    boolean reqAccept(InstId instanceId, Proposal proposal) throws IOException;
}
