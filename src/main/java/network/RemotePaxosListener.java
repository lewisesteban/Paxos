package network;

import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;

import java.rmi.Remote;

public interface RemotePaxosListener extends ListenerRPCHandle, Remote {
}
