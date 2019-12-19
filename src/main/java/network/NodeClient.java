package network;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.*;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class NodeClient implements RemotePaxosNode, PaxosProposer {

    public static void main(String[] args) {
        try {
            NodeClient client = new NodeClient(null, 0, 0);
            System.out.println(client.getNewInstanceId());
            System.out.println(client.getNewInstanceId());
            System.out.println("last instance: " + client.getListener().getSnapshotLastInstanceId());
        } catch (NotBoundException | IOException e) {
            e.printStackTrace();
        }
    }

    private RemotePaxosProposer paxosProposer;
    private AcceptorRPCHandle acceptor;
    private ListenerRPCHandle listener;
    private MembershipRPCHandle membership;
    private int nodeId;
    private int fragmentId;

    public NodeClient(String host, int nodeId, int fragmentId) throws RemoteException, NotBoundException {
        this.nodeId = nodeId;
        this.fragmentId = fragmentId;
        Registry registry = LocateRegistry.getRegistry(host);
        paxosProposer = (RemotePaxosProposer) registry.lookup(NodeServer.getStubName("proposer", this));
        acceptor = (RemotePaxosAcceptor) registry.lookup(NodeServer.getStubName("acceptor", this));
        listener = (RemotePaxosListener) registry.lookup(NodeServer.getStubName("listener", this));
        membership = (RemotePaxosMembership) registry.lookup(NodeServer.getStubName("membership", this));
    }

    @Override
    public long getNewInstanceId() throws IOException {
        return paxosProposer.getNewInstanceId();
    }

    @Override
    public Result propose(Command command, long instanceId) throws IOException {
        return paxosProposer.propose(command, instanceId);
    }

    @Override
    public void endClient(String clientId) throws IOException {
        paxosProposer.endClient(clientId);
    }

    @Override
    public int getId() {
        return nodeId;
    }

    @Override
    public int getFragmentId() {
        return fragmentId;
    }

    @Override
    public AcceptorRPCHandle getAcceptor() {
        return acceptor;
    }

    @Override
    public ListenerRPCHandle getListener() {
        return listener;
    }

    @Override
    public MembershipRPCHandle getMembership() {
        return membership;
    }
}