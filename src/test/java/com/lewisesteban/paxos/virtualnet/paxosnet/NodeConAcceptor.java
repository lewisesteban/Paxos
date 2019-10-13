package com.lewisesteban.paxos.virtualnet.paxosnet;

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
    public boolean reqAccept(long instanceId, Proposal proposal) throws IOException {
        return parent.tryNetCall(() -> acceptorHandle().reqAccept(instanceId, proposal));
    }

    @Override
    public long getLastInstance() throws IOException {
        return parent.tryNetCall(() -> acceptorHandle().getLastInstance());
    }
}
