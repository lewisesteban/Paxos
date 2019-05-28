package com.lewisesteban.paxos.virtualnet.paxosnet;

import com.lewisesteban.paxos.node.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.node.proposer.Proposal;
import com.lewisesteban.paxos.rpc.AcceptorRPCHandle;
import com.lewisesteban.paxos.virtualnet.VirtualConnection;

import java.io.IOException;

class NodeConAcceptor implements AcceptorRPCHandle {

    private VirtualConnection parent;
    private AcceptorRPCHandle paxosHandle;

    NodeConAcceptor(VirtualConnection parent, AcceptorRPCHandle paxosHandle) {
        this.parent = parent;
        this.paxosHandle = paxosHandle;
    }

    @Override
    public PrepareAnswer reqPrepare(int instanceId, Proposal.ID propId) throws IOException {
        parent.tryNetCall();
        return paxosHandle.reqPrepare(instanceId, propId);
    }

    @Override
    public boolean reqAccept(int instanceId, Proposal proposal) throws IOException {
        parent.tryNetCall();
        return paxosHandle.reqAccept(instanceId, proposal);
    }

    @Override
    public int getLastInstance() throws IOException {
        return paxosHandle.getLastInstance();
    }
}
