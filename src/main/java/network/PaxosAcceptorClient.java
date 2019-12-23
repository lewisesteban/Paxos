package network;

import com.lewisesteban.paxos.paxosnode.acceptor.AcceptAnswer;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;

import java.io.IOException;

public class PaxosAcceptorClient implements RemotePaxosAcceptor {
    private RemoteCallManager client;
    private AcceptorRPCHandle remoteAcceptor;

    PaxosAcceptorClient(AcceptorRPCHandle remoteAcceptor, RemoteCallManager client) {
        this.client = client;
        this.remoteAcceptor = remoteAcceptor;
    }

    @Override
    public PrepareAnswer reqPrepare(long instanceId, Proposal.ID propId) throws IOException {
        return client.doRemoteCall(() -> remoteAcceptor.reqPrepare(instanceId, propId));
    }

    @Override
    public AcceptAnswer reqAccept(long instanceId, Proposal proposal) throws IOException {
        return client.doRemoteCall(() -> remoteAcceptor.reqAccept(instanceId, proposal));
    }

    @Override
    public long getLastInstance() throws IOException {
        return client.doRemoteCall(() -> remoteAcceptor.getLastInstance());
    }
}
