package com.lewisesteban.paxos;

import com.lewisesteban.paxos.paxosnode.PaxosNode;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.storage.*;
import com.lewisesteban.paxos.storage.virtual.InterruptibleVirtualFileAccessor;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.VirtualNetNode;
import com.lewisesteban.paxos.virtualnet.paxosnet.NodeConnection;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

@SuppressWarnings({"WeakerAccess", "unused"})
public class NetworkFactory {

    public static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, int nbRacks, Network network, PaxosFactory paxosFactory, Iterable<Callable<StateMachine>> stateMachineCreators) {

        List<List<RemotePaxosNode>> networkViews = new ArrayList<>();
        for (int i = 0; i < totalNbNodes; ++i) {
            networkViews.add(new ArrayList<>());
        }

        int nodeId = 0;
        List<PaxosNetworkNode> paxosNodes = new ArrayList<>();
        Iterator<Callable<StateMachine>> executorIt = stateMachineCreators.iterator();
        for (List<RemotePaxosNode> networkView : networkViews) {
            final int thisNodeId = nodeId;
            final Callable<StateMachine> stateMachineCreator = executorIt.next();
            final StorageUnit.Creator storageUnitCreator = (file, dir) -> new SafeSingleFileStorage(file, dir, InterruptibleVirtualFileAccessor.creator(thisNodeId));
            final FileAccessorCreator fileAccessorCreator = InterruptibleVirtualFileAccessor.creator(thisNodeId);
            Callable<PaxosNode> paxosNodeCreator = () -> {
                StateMachine stateMachine = stateMachineCreator.call();
                stateMachine.setup(thisNodeId);
                return paxosFactory.create(thisNodeId, networkView, stateMachine, storageUnitCreator, fileAccessorCreator);
            };
            PaxosServer srv = new PaxosServer(paxosNodeCreator);
            int rack = srv.getId() % nbRacks;
            paxosNodes.add(new PaxosNetworkNode(srv, rack));
            nodeId++;
        }

        Iterator<PaxosNetworkNode> refNodeIt = paxosNodes.iterator();
        for (List<RemotePaxosNode> networkView : networkViews) {
            PaxosNetworkNode refNode = refNodeIt.next();
            for (PaxosNetworkNode connectedNode : paxosNodes) {
                networkView.add(new NodeConnection(connectedNode, refNode.getAddress(), network));
            }
        }

        for (VirtualNetNode node : paxosNodes) {
            network.addNode(node);
        }
        network.startAll();
        return paxosNodes;
    }

    public static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, Network network, PaxosFactory paxosFactory, Iterable<Callable<StateMachine>> stateMachineCreators) {
        return initSimpleNetwork(totalNbNodes, 1, network, paxosFactory, stateMachineCreators);
    }

    public static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, int nbRacks, Network network, Iterable<Callable<StateMachine>> stateMachineCreators) {
        return initSimpleNetwork(totalNbNodes, nbRacks, network, PaxosNode::new, stateMachineCreators);
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
        PaxosNode create(int nodeId, List<RemotePaxosNode> networkView, StateMachine stateMachine, StorageUnit.Creator storage, FileAccessorCreator fileAccessorCreator) throws StorageException;
    }

    public abstract static class BasicStateMachine implements StateMachine {
        protected int nodeId;
        Long waitingSnapshot = null;
        Long appliedSnapshot = null;
        StorageUnit storage;

        @Override
        public void setup(int nodeId) throws IOException {
            this.nodeId = nodeId;
            try {
                storage = new SafeSingleFileStorage("stateMachine" + nodeId, null, InterruptibleVirtualFileAccessor.creator(nodeId));
            } catch (StorageException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void createWaitingSnapshot(long idOfLastExecutedInstance) {
            waitingSnapshot = idOfLastExecutedInstance;
        }

        @Override
        public Snapshot getWaitingSnapshot() {
            if (waitingSnapshot == null)
                return null;
            return new Snapshot(waitingSnapshot, waitingSnapshot);
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
