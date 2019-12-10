package network;

import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;

import java.rmi.Remote;

public interface RemotePaxosAcceptor extends AcceptorRPCHandle, Remote {
}
