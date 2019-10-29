package com.lewisesteban.paxos.rpc.paxos;

import com.lewisesteban.paxos.paxosnode.acceptor.AcceptAnswer;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;

import java.io.IOException;

public interface AcceptorRPCHandle {

    PrepareAnswer reqPrepare(long instanceId, Proposal.ID propId) throws IOException;
    AcceptAnswer reqAccept(long instanceId, Proposal proposal) throws IOException;
    long getLastInstance() throws IOException;
}
