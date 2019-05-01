package com.lewisesteban.paxos.rpc;

import com.lewisesteban.paxos.node.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.node.proposer.Proposal;

import java.io.IOException;

public interface AcceptorRPCHandle {

    PrepareAnswer reqPrepare(Proposal.ID propId) throws IOException;
    boolean reqAccept(Proposal proposal) throws IOException;
}
