package com.lewisesteban.paxos.virtualnet.server;

import com.lewisesteban.paxos.node.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.node.proposer.Proposal;
import com.lewisesteban.paxos.rpc.AcceptorRPCHandle;

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
    public boolean reqAccept(long instanceId, Proposal proposal) throws IOException {
        return threadManager.pleaseDo(() -> paxosAcceptor.reqAccept(instanceId, proposal));
    }

}
