package network;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;

import java.io.IOException;

public class PaxosListenerSrv implements RemotePaxosListener {
    private ListenerRPCHandle paxosListener;

    PaxosListenerSrv(ListenerRPCHandle paxosListener) {
        this.paxosListener = paxosListener;
    }

    @Override
    public boolean execute(long instanceId, Command command) throws IOException {
        return paxosListener.execute(instanceId, command);
    }

    @Override
    public StateMachine.Snapshot getSnapshot() throws IOException {
        return paxosListener.getSnapshot();
    }

    @Override
    public long getSnapshotLastInstanceId() throws IOException {
        return paxosListener.getSnapshotLastInstanceId();
    }

    @Override
    public void gossipUnneededInstances(long[] unneededInstancesOfNodes) throws IOException {
        paxosListener.gossipUnneededInstances(unneededInstancesOfNodes);
    }
}
