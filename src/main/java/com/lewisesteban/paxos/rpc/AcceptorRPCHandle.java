package com.lewisesteban.paxos.rpc;

import com.lewisesteban.paxos.node.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.node.proposer.Proposal;

import java.io.IOException;

public interface AcceptorRPCHandle {

    PrepareAnswer reqPrepare(int instanceId, Proposal.ID propId) throws IOException;
    boolean reqAccept(int instanceId, Proposal proposal) throws IOException;
    int getLastInstance() throws IOException;
}
