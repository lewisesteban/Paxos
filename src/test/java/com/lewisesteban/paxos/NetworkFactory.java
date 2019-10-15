package com.lewisesteban.paxos;

import com.lewisesteban.paxos.paxosnode.PaxosNode;
import com.lewisesteban.paxos.storage.*;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.VirtualNetNode;
import com.lewisesteban.paxos.virtualnet.paxosnet.NodeConnection;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

@SuppressWarnings({"WeakerAccess", "unused"})
public class NetworkFactory {

    public static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, int nbRacks, Network network, PaxosFactory paxosFactory, Iterable<Callable<StateMachine>> stateMachineCreators) {

        InterruptibleTestStorage.Container.deleteAllFiles();

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
            final Callable<StorageUnit> storageCreator = () -> new InterruptibleTestStorage(thisNodeId, new SafeSingleFileStorage("paxosData" + thisNodeId, InterruptibleWholeFileAccessor.creator(true)));
            Callable<PaxosNode> paxosNodeCreator = () -> paxosFactory.create(thisNodeId, networkView, stateMachineCreator.call(), storageCreator.call());
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
            stateMachines.add(() -> (data) -> null);
        }
        return stateMachines;
    }

    public static Iterable<Callable<StateMachine>> stateMachinesAppendOK(int nb) {
        List<Callable<StateMachine>> stateMachines = new LinkedList<>();
        for (int i = 0; i < nb; ++i) {
            stateMachines.add(() -> (data) -> data.toString() + "OK");
        }
        return stateMachines;
    }

    public static Iterable<Callable<StateMachine>> stateMachinesSingle(Callable<StateMachine> stateMachineCreator, int nb) {
        List<Callable<StateMachine>> stateMachines = new LinkedList<>();
        stateMachines.add(stateMachineCreator);
        for (int i = 0; i < nb - 1; ++i) {
            stateMachines.add(() -> (data) -> null);
        }
        return stateMachines;
    }

    interface PaxosFactory {
        PaxosNode create(int nodeId, List<RemotePaxosNode> networkView, StateMachine stateMachine, StorageUnit storage) throws StorageException;
    }
}
