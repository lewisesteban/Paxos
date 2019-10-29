package com.lewisesteban.paxos.virtualnet.server;

import com.lewisesteban.paxos.paxosnode.acceptor.AcceptAnswer;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;

import java.io.IOException;

public class PaxosSrvAcceptor implements AcceptorRPCHandle {

    private AcceptorRPCHandle paxosAcceptor;
    private final PaxosServer.ThreadManager threadManager;

    PaxosSrvAcceptor(AcceptorRPCHandle paxosAcceptor, PaxosServer.ThreadManager threadManager) {
        this.paxosAcceptor = paxosAcceptor;
        this.threadManager = threadManager;
    }

    @Override
    public PrepareAnswer reqPrepare(long instanceId, Proposal.ID propId) throws IOException {
        return threadManager.pleaseDo(() -> paxosAcceptor.reqPrepare(instanceId, propId));
    }

    @Override
    public AcceptAnswer reqAccept(long instanceId, Proposal proposal) throws IOException {
        return threadManager.pleaseDo(() -> paxosAcceptor.reqAccept(instanceId, proposal));
    }

    @Override
    public long getLastInstance() throws IOException {
        return threadManager.pleaseDo(() -> paxosAcceptor.getLastInstance());
    }

}
