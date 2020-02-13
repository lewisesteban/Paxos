package network;

import com.lewisesteban.paxos.paxosnode.acceptor.AcceptAnswer;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;

import java.io.IOException;

public class PaxosAcceptorClient implements RemotePaxosAcceptor {
    private RemoteCallManager client;
    private AcceptorRPCHandle remoteAcceptor;
    private BulkCatchingUpManager catchingUpManager;

    PaxosAcceptorClient(AcceptorRPCHandle remoteAcceptor, RemoteCallManager client) {
        this.client = client;
        this.remoteAcceptor = remoteAcceptor;
        this.catchingUpManager = new BulkCatchingUpManager(this);
    }

    @Override
    public PrepareAnswer reqPrepare(long instanceId, Proposal.ID propId) throws IOException {
        RemoteCallManager.RemoteCallable<PrepareAnswer> normalCall =
                () -> client.doRemoteCall(() -> remoteAcceptor.reqPrepare(instanceId, propId));
        if (catchingUpManager.isCatchingUp())
            return catchingUpManager.sendRequest(instanceId, normalCall, propId, true);
        else
            return normalCall.doRemoteCall();
    }

    @Override
    public AcceptAnswer reqAccept(long instanceId, Proposal proposal) throws IOException {
        RemoteCallManager.RemoteCallable<AcceptAnswer> normalCall =
                () -> client.doRemoteCall(() -> remoteAcceptor.reqAccept(instanceId, proposal));
        if (catchingUpManager.isCatchingUp())
            return catchingUpManager.sendRequest(instanceId, normalCall, proposal, false);
        else
            return normalCall.doRemoteCall();
    }

    @Override
    public PrepareAnswer[] bulkPrepare(long[] instanceIds, Proposal.ID[] propIds) throws IOException {
        return client.doRemoteCall(() -> remoteAcceptor.bulkPrepare(instanceIds, propIds));
    }

    @Override
    public AcceptAnswer[] bulkAccept(long[] instanceIds, Proposal[] proposals) throws IOException {
        return client.doRemoteCall(() -> remoteAcceptor.bulkAccept(instanceIds, proposals));
    }

    @Override
    public long getLastInstance() throws IOException {
        return client.doRemoteCall(() -> remoteAcceptor.getLastInstance());
    }
}
