package network;

import com.lewisesteban.paxos.paxosnode.acceptor.AcceptAnswer;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;

import java.io.IOException;

public class RemotePaxosAcceptorImpl implements RemotePaxosAcceptor {
    private AcceptorRPCHandle paxosAcceptor;

    RemotePaxosAcceptorImpl(AcceptorRPCHandle paxosAcceptor) {
        this.paxosAcceptor = paxosAcceptor;
    }

    @Override
    public PrepareAnswer reqPrepare(long instanceId, Proposal.ID propId) throws IOException {
        return paxosAcceptor.reqPrepare(instanceId, propId);
    }

    @Override
    public AcceptAnswer reqAccept(long instanceId, Proposal proposal) throws IOException {
        return paxosAcceptor.reqAccept(instanceId, proposal);
    }

    @Override
    public long getLastInstance() throws IOException {
        return paxosAcceptor.getLastInstance();
    }
}
