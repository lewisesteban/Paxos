package network;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;

import java.io.IOException;

public class PaxosListenerClient implements RemotePaxosListener {
    private RemoteCallManager client;
    private ListenerRPCHandle remoteListener;

    PaxosListenerClient(ListenerRPCHandle remoteListener, RemoteCallManager client) {
        this.remoteListener = remoteListener;
        this.client = client;
    }

    @Override
    public boolean execute(long instanceId, Command command) throws IOException {
        return client.doRemoteCall(() -> remoteListener.execute(instanceId, command));
    }

    @Override
    public StateMachine.Snapshot getSnapshot() throws IOException {
        return client.doRemoteCall(() -> remoteListener.getSnapshot());
    }

    @Override
    public long getSnapshotLastInstanceId() throws IOException {
        return client.doRemoteCall(() -> remoteListener.getSnapshotLastInstanceId());
    }

    @Override
    public void gossipUnneededInstances(long[] unneededInstancesOfNodes) throws IOException {
        client.doRemoteCall(() -> {
            remoteListener.gossipUnneededInstances(unneededInstancesOfNodes);
            return true;
        });
    }
}
