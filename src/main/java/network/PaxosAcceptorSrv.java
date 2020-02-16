package network;

import com.lewisesteban.paxos.paxosnode.acceptor.AcceptAnswer;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;

import java.io.IOException;

public class PaxosAcceptorSrv implements RemotePaxosAcceptor {
    private AcceptorRPCHandle paxosAcceptor;

    PaxosAcceptorSrv(AcceptorRPCHandle paxosAcceptor) {
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
    public PrepareAnswer[] bulkPrepare(long[] instanceIds, Proposal.ID[] propIds) throws IOException {
        return paxosAcceptor.bulkPrepare(instanceIds, propIds);
    }

    @Override
    public AcceptAnswer[] bulkAccept(long[] instanceIds, Proposal[] proposals) throws IOException {
        return paxosAcceptor.bulkAccept(instanceIds, proposals);
    }

    @Override
    public long getLastInstance() throws IOException {
        return paxosAcceptor.getLastInstance();
    }

    @Override
    public long getLastPropNb() throws IOException {
        return paxosAcceptor.getLastPropNb();
    }
}
