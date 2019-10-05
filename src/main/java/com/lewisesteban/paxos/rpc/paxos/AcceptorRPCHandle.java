package com.lewisesteban.paxos.rpc.paxos;

import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;

import java.io.IOException;

public interface AcceptorRPCHandle {

    PrepareAnswer reqPrepare(int instanceId, Proposal.ID propId) throws IOException;
    boolean reqAccept(int instanceId, Proposal proposal) throws IOException;
    int getLastInstance() throws IOException;
}
