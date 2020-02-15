package network;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;

import java.io.IOException;

class EmptyListener implements ListenerRPCHandle {
    @Override
    public boolean execute(long instanceId, Command command) throws IOException {
        throw new IOException("Not connected");
    }

    @Override
    public StateMachine.Snapshot getSnapshot() throws IOException {
        throw new IOException("Not connected");
    }

    @Override
    public long getSnapshotLastInstanceId() throws IOException {
        throw new IOException("Not connected");
    }

    @Override
    public void gossipUnneededInstances(long[] unneededInstancesOfNodes) throws IOException {
        throw new IOException("Not connected");
    }
}
