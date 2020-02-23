package com.lewisesteban.paxos;

import com.lewisesteban.paxos.paxosnode.PaxosNode;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.storage.FileAccessorCreator;
import com.lewisesteban.paxos.storage.SafeSingleFileStorage;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;
import com.lewisesteban.paxos.storage.virtual.InterruptibleVirtualFileAccessor;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.VirtualNetNode;
import com.lewisesteban.paxos.virtualnet.paxosnet.NodeConnection;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

@SuppressWarnings({"unused"})
public class NetworkFactory {

    /**
     * Creates paxos nodes, connects them and adds them to a network. All these nodes will be part of a single cluster
     * (responsible for a single fragment).
     * Returns the nodes of the cluster.
     */
    public static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, int nbRacks, Network network, PaxosFactory paxosFactory, Iterable<Callable<StateMachine>> stateMachineCreators, int fragmentNb) {

        List<List<RemotePaxosNode>> networkViews = new ArrayList<>();
        for (int i = 0; i < totalNbNodes; ++i) {
            networkViews.add(new ArrayList<>());
        }

        int nodeId = 0;
        List<PaxosNetworkNode> networkNodes = new ArrayList<>();
        Iterator<Callable<StateMachine>> stateMachineIt = stateMachineCreators.iterator();
        for (List<RemotePaxosNode> networkView : networkViews) {
            final int thisNodeId = nodeId;
            final Callable<StateMachine> stateMachineCreator = stateMachineIt.next();
            final StorageUnit.Creator storageUnitCreator = (file, dir) -> new SafeSingleFileStorage(file, dir, InterruptibleVirtualFileAccessor.creator(thisNodeId));
            final FileAccessorCreator fileAccessorCreator = InterruptibleVirtualFileAccessor.creator(thisNodeId);
            Callable<PaxosNode> paxosNodeCreator = () -> {
                StateMachine stateMachine = stateMachineCreator.call();
                stateMachine.setup(Integer.toString(thisNodeId));
                return paxosFactory.create(thisNodeId, fragmentNb, networkView, stateMachine, storageUnitCreator, fileAccessorCreator);
            };
            PaxosServer srv = new PaxosServer(paxosNodeCreator);
            int rack = srv.getId() % nbRacks;
            networkNodes.add(new PaxosNetworkNode(srv, rack));
            nodeId++;
        }

        Iterator<PaxosNetworkNode> refNodeIt = networkNodes.iterator();
        for (List<RemotePaxosNode> networkView : networkViews) {
            PaxosNetworkNode refNode = refNodeIt.next();
            for (PaxosNetworkNode connectedNode : networkNodes) {
                networkView.add(new NodeConnection(connectedNode, refNode.getAddress(), network));
            }
        }

        for (VirtualNetNode node : networkNodes) {
            network.addNode(node);
        }
        network.startAll();
        return networkNodes;
    }

    public static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, Network network, PaxosFactory paxosFactory, Iterable<Callable<StateMachine>> stateMachineCreators) {
        return initSimpleNetwork(totalNbNodes, 1, network, paxosFactory, stateMachineCreators, 0);
    }

    public static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, Network network, PaxosFactory paxosFactory, Iterable<Callable<StateMachine>> stateMachineCreators, int fragmentNb) {
        return initSimpleNetwork(totalNbNodes, 1, network, paxosFactory, stateMachineCreators, fragmentNb);
    }

    public static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, Network network, Iterable<Callable<StateMachine>> stateMachineCreators, int fragmentNb) {
        return initSimpleNetwork(totalNbNodes, 1, network, stateMachineCreators, fragmentNb);
    }

    public static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, int nbRacks, Network network, Iterable<Callable<StateMachine>> stateMachineCreators) {
        return initSimpleNetwork(totalNbNodes, nbRacks, network, PaxosNode::new, stateMachineCreators, 0);
    }

    public static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, int nbRacks, Network network, Iterable<Callable<StateMachine>> stateMachineCreators, int fragmentNb) {
        return initSimpleNetwork(totalNbNodes, nbRacks, network, PaxosNode::new, stateMachineCreators, fragmentNb);
    }

    public static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, Network network, Iterable<Callable<StateMachine>> stateMachineCreators) {
        return initSimpleNetwork(totalNbNodes, network, PaxosNode::new, stateMachineCreators);
    }

    public static Iterable<Callable<StateMachine>> stateMachinesSame(Callable<StateMachine> stateMachineCreator, int nb) {
        List<Callable<StateMachine>> stateMachines = new LinkedList<>();
        for (int i = 0; i < nb; ++i) {
            stateMachines.add(stateMachineCreator);
        }
        return stateMachines;
    }

    public static Iterable<Callable<StateMachine>> stateMachinesEmpty(int nb) {
        List<Callable<StateMachine>> stateMachines = new LinkedList<>();
        for (int i = 0; i < nb; ++i) {
            stateMachines.add(basicStateMachine((data) -> null));
        }
        return stateMachines;
    }

    public static Iterable<Callable<StateMachine>> stateMachinesAppendOK(int nb) {
        List<Callable<StateMachine>> stateMachines = new LinkedList<>();
        for (int i = 0; i < nb; ++i) {
            stateMachines.add(basicStateMachine((data) -> data.toString() + "OK"));
        }
        return stateMachines;
    }

    public static Iterable<Callable<StateMachine>> stateMachinesMirror(int nb) {
        List<Callable<StateMachine>> stateMachines = new LinkedList<>();
        for (int i = 0; i < nb; ++i) {
            stateMachines.add(basicStateMachine((data) -> data));
        }
        return stateMachines;
    }

    public static Iterable<Callable<StateMachine>> stateMachinesIncrement(int nb) {
        List<Callable<StateMachine>> stateMachines = new LinkedList<>();
        for (int i = 0; i < nb; ++i) {
            stateMachines.add(basicStateMachine((data) -> ((int)data) + 1));
        }
        return stateMachines;
    }

    public static Iterable<Callable<StateMachine>> stateMachinesSingle(Callable<StateMachine> stateMachineCreator, int nb) {
        List<Callable<StateMachine>> stateMachines = new LinkedList<>();
        stateMachines.add(stateMachineCreator);
        for (int i = 0; i < nb - 1; ++i) {
            stateMachines.add(basicStateMachine((data) -> null));
        }
        return stateMachines;
    }

    public static Callable<StateMachine> basicStateMachine(BasicStateMachine.CommandExecutor executeCmd) {
        return () -> new BasicStateMachine() {
            @Override
            public Serializable execute(Serializable data) {
                return executeCmd.execute(data);
            }
        };
    }

    interface PaxosFactory {
        PaxosNode create(int nodeId, int fragmentId, List<RemotePaxosNode> networkView, StateMachine stateMachine, StorageUnit.Creator storage, FileAccessorCreator fileAccessorCreator) throws StorageException;
    }

    public abstract static class BasicStateMachine implements StateMachine {
        protected String nodeId;
        Long waitingSnapshot = null;
        Long appliedSnapshot = null;
        StorageUnit storage;

        @Override
        public void setup(String id) {
            this.nodeId = id;
            try {
                storage = new SafeSingleFileStorage("stateMachine" + nodeId, null, InterruptibleVirtualFileAccessor.creator(Integer.parseInt(nodeId)));
            } catch (StorageException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void createWaitingSnapshot(long idOfLastExecutedInstance) {
            waitingSnapshot = idOfLastExecutedInstance;
        }

        @Override
        public Snapshot getAppliedSnapshot() throws StorageException {
            if (storage.isEmpty())
                return null;
            appliedSnapshot = Long.parseLong(storage.read("inst"));
            return new Snapshot(appliedSnapshot, appliedSnapshot);
        }

        @Override
        public long getWaitingSnapshotLastInstance() {
            return waitingSnapshot;
        }

        @Override
        public long getAppliedSnapshotLastInstance() throws StorageException {
            if (storage.isEmpty())
                return -1;
            appliedSnapshot = Long.parseLong(storage.read("inst"));
            return appliedSnapshot;
        }

        @Override
        public void applySnapshot(Snapshot snapshot) throws StorageException {
            waitingSnapshot = null;
            appliedSnapshot = snapshot.getLastIncludedInstance();
            storage.put("inst", Long.toString(appliedSnapshot));
            storage.flush();
        }

        @Override
        public void applyCurrentWaitingSnapshot() throws StorageException {
            appliedSnapshot = waitingSnapshot;
            waitingSnapshot = null;
            storage.put("inst", Long.toString(appliedSnapshot));
            storage.flush();
        }

        @Override
        public boolean hasWaitingSnapshot() {
            return waitingSnapshot != null;
        }

        @Override
        public boolean hasAppliedSnapshot() throws StorageException {
            return !storage.isEmpty();
        }

        public interface CommandExecutor {
            Serializable execute(Serializable data);
        }
    }
}
