package com.lewisesteban.paxos.virtualnet.node;

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

    public PrepareAnswer reqPrepare(Proposal.ID propId) throws IOException {
        parent.tryNetCall();
        return paxosHandle.reqPrepare(propId);
    }

    public boolean reqAccept(Proposal proposal) throws IOException {
        parent.tryNetCall();
        return paxosHandle.reqAccept(proposal);
    }
}
