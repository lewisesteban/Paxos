package network;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.*;

import java.io.IOException;
import java.rmi.ConnectException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class NodeClient implements RemotePaxosNode, PaxosProposer, RemoteCallManager {
    private String host;
    private RemotePaxosProposer paxosProposer;
    private AcceptorRPCHandle acceptor;
    private ListenerRPCHandle listener;
    private MembershipRPCHandle membership;
    private int nodeId;
    private int fragmentId;
    private boolean connected = false;

    public NodeClient(String host, int nodeId, int fragmentId) {
        this.nodeId = nodeId;
        this.fragmentId = fragmentId;
        this.host = host;

        try {
            connectToServer();
        } catch (RemoteException | NotBoundException e) {
            connected = false;
        }
    }

    private void connectToServer() throws RemoteException, NotBoundException {
        Registry registry = LocateRegistry.getRegistry(host);
        paxosProposer = (RemotePaxosProposer) registry.lookup(NodeServer.getStubName("proposer", this));
        acceptor = new PaxosAcceptorClient((RemotePaxosAcceptor) registry.lookup(NodeServer.getStubName("acceptor", this)), this);
        listener = new PaxosListenerClient((RemotePaxosListener) registry.lookup(NodeServer.getStubName("listener", this)), this);
        membership = new PaxosMembershipClient((RemotePaxosMembership) registry.lookup(NodeServer.getStubName("membership", this)), this);
        connected = true;
    }

    public <T> T doRemoteCall(RemoteCallable<T> callable) throws IOException {
        try {
            if (!connected)
                connectToServer();
            return callable.doRemoteCall();
        } catch (ConnectException | NoSuchObjectException e) {
            connected = false;
            throw e;
        } catch (NotBoundException e) {
            throw new IOException(e);
        }
    }

    @Override
    public long getNewInstanceId() throws IOException {
        return doRemoteCall(() -> paxosProposer.getNewInstanceId());
    }

    @Override
    public Result propose(Command command, long instanceId) throws IOException {
        return doRemoteCall(() -> paxosProposer.propose(command, instanceId));
    }

    @Override
    public void endClient(String clientId) throws IOException {
        doRemoteCall(() -> {
            paxosProposer.endClient(clientId);
            return true;
        });
    }

    @Override
    public int getId() {
        return nodeId;
    }

    @Override
    public int getFragmentId() {
        return fragmentId;
    }

    /*
     * The following three methods should always return a non-null object.
     * Any attempt on using an "Empty" object will cause "doRemoteCall" to attempt connection, because "connected" will,
     * in this case, always be false.
     */

    @Override
    public AcceptorRPCHandle getAcceptor() {
        if (acceptor == null)
            return new EmptyAcceptor();
        return acceptor;
    }

    @Override
    public ListenerRPCHandle getListener() {
        if (listener == null)
            return new EmptyListener();
        return listener;
    }

    @Override
    public MembershipRPCHandle getMembership() {
        if (membership == null)
            return new EmptyMembership();
        return membership;
    }

}