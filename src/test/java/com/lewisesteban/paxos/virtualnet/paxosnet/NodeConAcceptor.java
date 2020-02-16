package com.lewisesteban.paxos.virtualnet.paxosnet;

import com.lewisesteban.paxos.paxosnode.acceptor.AcceptAnswer;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;
import com.lewisesteban.paxos.virtualnet.VirtualConnection;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;

import java.io.IOException;

class NodeConAcceptor implements AcceptorRPCHandle {

    private VirtualConnection parent;
    private PaxosServer paxosHandle;

    NodeConAcceptor(VirtualConnection parent, PaxosServer paxosHandle) {
        this.parent = parent;
        this.paxosHandle = paxosHandle;
    }

    private AcceptorRPCHandle acceptorHandle() {
        return paxosHandle.getAcceptor();
    }

    @Override
    public PrepareAnswer reqPrepare(long instanceId, Proposal.ID propId) throws IOException {
        return parent.tryNetCall(() -> acceptorHandle().reqPrepare(instanceId, propId));
    }

    @Override
    public AcceptAnswer reqAccept(long instanceId, Proposal proposal) throws IOException {
        return parent.tryNetCall(() -> acceptorHandle().reqAccept(instanceId, proposal));
    }

    @Override
    public PrepareAnswer[] bulkPrepare(long[] instanceIds, Proposal.ID[] propIds) throws IOException {
        return parent.tryNetCall(() -> acceptorHandle().bulkPrepare(instanceIds, propIds));
    }

    @Override
    public AcceptAnswer[] bulkAccept(long[] instanceIds, Proposal[] proposals) throws IOException {
        return parent.tryNetCall(() -> acceptorHandle().bulkAccept(instanceIds, proposals));
    }

    @Override
    public long getLastInstance() throws IOException {
        return parent.tryNetCall(() -> acceptorHandle().getLastInstance());
    }

    @Override
    public long getLastPropNb() throws IOException {
        return parent.tryNetCall(() -> acceptorHandle().getLastPropNb());
    }
}
