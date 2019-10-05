package com.lewisesteban.paxos;

import com.lewisesteban.paxos.paxosnode.PaxosNode;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.VirtualNetNode;
import com.lewisesteban.paxos.virtualnet.paxosnet.NodeConnection;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;

import java.util.*;

class NetworkFactory {

    static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, int nbRacks, Network network, PaxosFactory paxosFactory, Iterable<StateMachine> executors) {

        List<List<RemotePaxosNode>> networkViews = new ArrayList<>();
        for (int i = 0; i < totalNbNodes; ++i) {
            networkViews.add(new ArrayList<>());
        }

        int nodeId = 0;
        List<PaxosNetworkNode> paxosNodes = new ArrayList<>();
        Iterator<StateMachine> executorIt = executors.iterator();
        for (List<RemotePaxosNode> networkView : networkViews) {
            StateMachine stateMachine = executorIt.next();
            PaxosNode paxos = paxosFactory.createNode(nodeId, networkView, stateMachine);
            PaxosServer srv = new PaxosServer(paxos);
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

    static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, Network network, PaxosFactory paxosFactory, Iterable<StateMachine> executor) {
        return initSimpleNetwork(totalNbNodes, 1, network, paxosFactory, executor);
    }

    static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, int nbRacks, Network network, Iterable<StateMachine> executor) {
        return initSimpleNetwork(totalNbNodes, nbRacks, network, PaxosNode::new, executor);
    }

    static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, Network network, Iterable<StateMachine> executor) {
        return initSimpleNetwork(totalNbNodes, network, PaxosNode::new, executor);
    }

    static Iterable<StateMachine> executorsSame(StateMachine stateMachine, int nb) {
        List<StateMachine> stateMachines = new LinkedList<>();
        for (int i = 0; i < nb; ++i) {
            stateMachines.add(stateMachine);
        }
        return stateMachines;
    }

    static Iterable<StateMachine> executorsEmpty(int nb) {
        List<StateMachine> stateMachines = new LinkedList<>();
        for (int i = 0; i < nb; ++i) {
            stateMachines.add((data) -> null);
        }
        return stateMachines;
    }

    static Iterable<StateMachine> executorsSingle(StateMachine stateMachine, int nb) {
        List<StateMachine> stateMachines = new LinkedList<>();
        stateMachines.add(stateMachine);
        for (int i = 0; i < nb - 1; ++i) {
            stateMachines.add((data) -> null);
        }
        return stateMachines;
    }

    interface PaxosFactory {
        PaxosNode createNode(int nodeId, List<RemotePaxosNode> networkView, StateMachine stateMachine);
    }
}
