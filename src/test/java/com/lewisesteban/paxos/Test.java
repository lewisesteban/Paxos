package com.lewisesteban.paxos;

import com.lewisesteban.paxos.rpc.RemotePaxosNode;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.VirtualNetNode;
import com.lewisesteban.paxos.virtualnet.node.NodeConnection;
import com.lewisesteban.paxos.virtualnet.node.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Test extends TestCase {

    private List<PaxosNetworkNode> createVirtualNetwork(int totalNbNodes, Network network) {

        List<List<RemotePaxosNode>> networkViews = new ArrayList<>();
        for (int i = 0; i < totalNbNodes; ++i) {
            networkViews.add(new ArrayList<>());
        }

        int nodeId = 0;
        List<PaxosNetworkNode> paxosNodes = new ArrayList<>();
        for (List<RemotePaxosNode> networkView : networkViews) {
            PaxosNode paxos = new PaxosNode(nodeId, networkView);
            PaxosServer srv = new PaxosServer(paxos);
            paxosNodes.add(new PaxosNetworkNode(srv));
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

    public void testSimpleTwoProposals() throws IOException {
        Network network = new Network();
        List<PaxosNetworkNode> nodes = createVirtualNetwork(2, network);
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        assert node0.propose("ONE");
        assert !node0.propose("TWO");
    }
}
