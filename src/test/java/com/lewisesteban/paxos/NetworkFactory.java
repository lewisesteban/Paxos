package com.lewisesteban.paxos;

import com.lewisesteban.paxos.rpc.RemotePaxosNode;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.VirtualNetNode;
import com.lewisesteban.paxos.virtualnet.paxosnet.NodeConnection;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;

import java.util.*;

class NetworkFactory {

    static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, int nbRacks, Network network, PaxosFactory paxosFactory, Iterable<Executor> executors) {

        List<List<RemotePaxosNode>> networkViews = new ArrayList<>();
        for (int i = 0; i < totalNbNodes; ++i) {
            networkViews.add(new ArrayList<>());
        }

        int nodeId = 0;
        List<PaxosNetworkNode> paxosNodes = new ArrayList<>();
        Iterator<Executor> executorIt = executors.iterator();
        for (List<RemotePaxosNode> networkView : networkViews) {
            Executor executor = executorIt.next();
            PaxosNode paxos = paxosFactory.createNode(nodeId, networkView, executor);
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

    static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, Network network, PaxosFactory paxosFactory, Iterable<Executor> executor) {
        return initSimpleNetwork(totalNbNodes, 1, network, paxosFactory, executor);
    }

    static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, int nbRacks, Network network, Iterable<Executor> executor) {
        return initSimpleNetwork(totalNbNodes, nbRacks, network, PaxosNode::new, executor);
    }

    static List<PaxosNetworkNode> initSimpleNetwork(int totalNbNodes, Network network, Iterable<Executor> executor) {
        return initSimpleNetwork(totalNbNodes, network, PaxosNode::new, executor);
    }

    static Iterable<Executor> executorsSame(Executor executor, int nb) {
        List<Executor> executors = new LinkedList<>();
        for (int i = 0; i < nb; ++i) {
            executors.add(executor);
        }
        return executors;
    }

    static Iterable<Executor> executorsEmpty(int nb) {
        List<Executor> executors = new LinkedList<>();
        for (int i = 0; i < nb; ++i) {
            executors.add((instId, data) -> {});
        }
        return executors;
    }

    static Iterable<Executor> executorsSingle(Executor executor, int nb) {
        List<Executor> executors = new LinkedList<>();
        executors.add(executor);
        for (int i = 0; i < nb - 1; ++i) {
            executors.add((instId, data) -> {});
        }
        return executors;
    }

    interface PaxosFactory {
        PaxosNode createNode(int nodeId, List<RemotePaxosNode> networkView, Executor executor);
    }
}
