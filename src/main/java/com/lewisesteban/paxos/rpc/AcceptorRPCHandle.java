package com.lewisesteban.paxos.rpc;

import com.lewisesteban.paxos.node.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.node.proposer.Proposal;

import java.io.IOException;

public interface AcceptorRPCHandle {

    PrepareAnswer reqPrepare(long instanceId, Proposal.ID propId) throws IOException;
    boolean reqAccept(long instanceId, Proposal proposal) throws IOException;
}
