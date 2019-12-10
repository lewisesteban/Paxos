package network;

import com.lewisesteban.paxos.rpc.paxos.MembershipRPCHandle;

import java.rmi.Remote;

public interface RemotePaxosMembership extends MembershipRPCHandle, Remote {
}
