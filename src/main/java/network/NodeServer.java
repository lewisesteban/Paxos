package network;

import com.lewisesteban.paxos.paxosnode.PaxosNode;
import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.MembershipRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class NodeServer extends RemotePaxosProposerImpl implements RemotePaxosNode {
    private PaxosNode paxosNode;

    public NodeServer(PaxosNode paxosNode) throws RemoteException {
        super(paxosNode);
        this.paxosNode = paxosNode;
        RemotePaxosProposer proposerStub = (RemotePaxosProposer) UnicastRemoteObject.exportObject(this, 0);
        RemotePaxosAcceptor acceptorStub = (RemotePaxosAcceptor) UnicastRemoteObject.exportObject(new RemotePaxosAcceptorImpl(paxosNode.getAcceptor()), 0);
        RemotePaxosListener listenerStub = (RemotePaxosListener) UnicastRemoteObject.exportObject(new RemotePaxosListenerImpl(paxosNode.getListener()), 0);
        RemotePaxosMembership membershipStub = (RemotePaxosMembership) UnicastRemoteObject.exportObject(new RemotePaxosMembershipImpl(paxosNode.getMembership()), 0);
        Registry registry = LocateRegistry.getRegistry();
        registry.rebind(getStubName("proposer", this), proposerStub);
        registry.rebind(getStubName("acceptor", this), acceptorStub);
        registry.rebind(getStubName("listener", this), listenerStub);
        registry.rebind(getStubName("membership", this), membershipStub);
    }

    public void start() {
        paxosNode.start();
    }

    @Override
    public int getId() {
        return paxosNode.getId();
    }

    @Override
    public int getFragmentId() {
        return paxosNode.getFragmentId();
    }

    @Override
    public AcceptorRPCHandle getAcceptor() {
        return paxosNode.getAcceptor();
    }

    @Override
    public ListenerRPCHandle getListener() {
        return paxosNode.getListener();
    }

    @Override
    public MembershipRPCHandle getMembership() {
        return paxosNode.getMembership();
    }

    static String getStubName(String className, RemotePaxosNode remotePaxosNode) {
        return className + "_" + remotePaxosNode.getFragmentId() + "_" + remotePaxosNode.getId();
    }
}
