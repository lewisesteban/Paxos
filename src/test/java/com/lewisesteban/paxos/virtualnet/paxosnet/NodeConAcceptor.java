package com.lewisesteban.paxos.virtualnet.paxosnet;

import com.lewisesteban.paxos.InstId;
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

    public PrepareAnswer reqPrepare(InstId instanceId, Proposal.ID propId) throws IOException {
        parent.tryNetCall();
        return paxosHandle.reqPrepare(instanceId, propId);
    }

    public boolean reqAccept(InstId instanceId, Proposal proposal) throws IOException {
        parent.tryNetCall();
        return paxosHandle.reqAccept(instanceId, proposal);
    }
}
