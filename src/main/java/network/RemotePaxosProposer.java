package network;

import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;

import java.rmi.Remote;

public interface RemotePaxosProposer extends PaxosProposer, Remote {
}
