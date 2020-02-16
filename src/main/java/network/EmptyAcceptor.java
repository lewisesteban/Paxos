package network;

import com.lewisesteban.paxos.paxosnode.acceptor.AcceptAnswer;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;

import java.io.IOException;

class EmptyAcceptor implements AcceptorRPCHandle {

    @Override
    public PrepareAnswer reqPrepare(long instanceId, Proposal.ID propId) throws IOException {
        throw new IOException("Not connected");
    }

    @Override
    public AcceptAnswer reqAccept(long instanceId, Proposal proposal) throws IOException {
        throw new IOException("Not connected");
    }

    @Override
    public PrepareAnswer[] bulkPrepare(long[] instanceIds, Proposal.ID[] propIds) throws IOException {
        throw new IOException("Not connected");
    }

    @Override
    public AcceptAnswer[] bulkAccept(long[] instanceIds, Proposal[] proposals) throws IOException {
        throw new IOException("Not connected");
    }

    @Override
    public long getLastInstance() throws IOException {
        throw new IOException("Not connected");
    }

    @Override
    public long getLastPropNb() throws IOException {
        throw new IOException("Not connected");
    }
}
