package network;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.listener.CatchingUpManager;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.*;

import java.io.IOException;
import java.rmi.ConnectException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class NodeClient implements RemotePaxosNode, PaxosProposer, RemoteCallManager {
    private String host;
    private RemotePaxosProposer paxosProposer;
    private AcceptorRPCHandle acceptor;
    private ListenerRPCHandle listener;
    private MembershipRPCHandle membership;
    private CatchingUpManager catchingUpManager = null;
    private int nodeId;
    private int fragmentId;
    private boolean connected = false;

    public NodeClient(String host, int nodeId, int fragmentId) {
        this.nodeId = nodeId;
        this.fragmentId = fragmentId;
        this.host = host;
        new Thread(() -> {
            try {
                connectToServer();
            } catch (IOException ignored) {
            }
        }).start();
    }

    private synchronized void connectToServer() throws IOException {
        if (!connected) {
            try {
                Registry registry = LocateRegistry.getRegistry(host);
                paxosProposer = (RemotePaxosProposer) registry.lookup(NodeServer.getStubName("proposer", this));
                PaxosAcceptorClient paxosAcceptorClient = new PaxosAcceptorClient((RemotePaxosAcceptor) registry.lookup(NodeServer.getStubName("acceptor", this)), this);
                acceptor = paxosAcceptorClient;
                listener = new PaxosListenerClient((RemotePaxosListener) registry.lookup(NodeServer.getStubName("listener", this)), this);
                membership = new PaxosMembershipClient((RemotePaxosMembership) registry.lookup(NodeServer.getStubName("membership", this)), this);
                catchingUpManager = paxosAcceptorClient.getCatchingUpManager();
                connected = true;
            } catch (ConnectException | NoSuchObjectException | NotBoundException e) {
                connected = false;
                throw new IOException(e);
            }
        }
    }

    public <T> T doRemoteCall(RemoteCallable<T> callable) throws IOException {
        if (!connected)
            connectToServer();
        return callable.doRemoteCall();

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
        if (acceptor == null) {
            if (tryConnect())
                return acceptor;
            else
                return new EmptyAcceptor();
        }
        return acceptor;
    }

    @Override
    public ListenerRPCHandle getListener() {
        if (listener == null) {
            if (tryConnect())
                return listener;
            else
                return new EmptyListener();
        }
        return listener;
    }

    @Override
    public MembershipRPCHandle getMembership() {
        if (membership == null) {
            if (tryConnect())
                return membership;
            else
                return new EmptyMembership();
        }
        return membership;
    }

    private boolean tryConnect() {
        try {
            connectToServer();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public MultiClientCatchingUpManager.ClientCUMGetter getCatchingUpManager() {
        return () -> this.catchingUpManager;
    }
}