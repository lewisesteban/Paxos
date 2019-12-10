package network;

import com.lewisesteban.paxos.paxosnode.PaxosNode;
import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.MembershipRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.storage.SafeSingleFileStorage;
import com.lewisesteban.paxos.storage.WholeFileAccessor;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

public class NodeServer extends RemotePaxosProposerImpl implements RemotePaxosNode {
    public static void main(String args[]) {

        try {
            // LocateRegistry.createRegistry(1099);
            // "rmiregistry" should be started before launching the program, and in the root folder containing compiled
            // files. In IntelliJ, that would be "target/classes".

            List<RemotePaxosNode> list = new ArrayList<>();
            PaxosNode paxosNode = new PaxosNode(0, 0, list,
                    new largetable.Server(WholeFileAccessor::new),
                    (file, dir) -> new SafeSingleFileStorage(file, dir, WholeFileAccessor::new),
                    WholeFileAccessor::new);
            list.add(paxosNode);
            paxosNode.start();
            new NodeServer(paxosNode);

            // Bind the remote object's stub in the registry

            System.err.println("Server ready");
        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
    }

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

    public static String getStubName(String className, RemotePaxosNode remotePaxosNode) {
        return className + "_" + remotePaxosNode.getFragmentId() + "_" + remotePaxosNode.getId();
    }
}
