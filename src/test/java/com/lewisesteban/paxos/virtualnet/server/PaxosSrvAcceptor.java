package com.lewisesteban.paxos.virtualnet.server;

import com.lewisesteban.paxos.node.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.node.proposer.Proposal;
import com.lewisesteban.paxos.rpc.AcceptorRPCHandle;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class PaxosSrvAcceptor implements AcceptorRPCHandle {

    private AcceptorRPCHandle paxosAcceptor;
    private ExecutorService threadPool;

    PaxosSrvAcceptor(AcceptorRPCHandle paxosAcceptor, ExecutorService threadPool) {
        this.paxosAcceptor = paxosAcceptor;
        this.threadPool = threadPool;
    }

    @Override
    public PrepareAnswer reqPrepare(Proposal.ID propId) throws IOException {
        try {
            return threadPool.submit(() -> paxosAcceptor.reqPrepare(propId)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean reqAccept(Proposal proposal) throws IOException {
        try {
            return threadPool.submit(() -> paxosAcceptor.reqAccept(proposal)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

}
