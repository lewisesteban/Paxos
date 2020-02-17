package com.lewisesteban.paxos.rpc.paxos;

import com.lewisesteban.paxos.paxosnode.acceptor.AcceptAnswer;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;

import java.io.IOException;
import java.rmi.Remote;

public interface AcceptorRPCHandle extends Remote {

    PrepareAnswer reqPrepare(long instanceId, Proposal.ID propId) throws IOException;
    AcceptAnswer reqAccept(long instanceId, Proposal proposal) throws IOException;
    PrepareAnswer[] bulkPrepare(long[] instanceIds, Proposal.ID[] propIds) throws IOException;
    AcceptAnswer[] bulkAccept(long[] instanceIds, Proposal[] proposals) throws IOException;
    long getLastInstance() throws IOException;
    long getLastPropNb() throws IOException;
}
