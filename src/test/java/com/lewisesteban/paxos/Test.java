package com.lewisesteban.paxos;

import com.lewisesteban.paxos.rpc.RemotePaxosNode;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.VirtualNode;
import com.lewisesteban.paxos.virtualnet.node.NodeConnection;
import com.lewisesteban.paxos.virtualnet.node.PaxosNodeWrapper;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Test extends TestCase {

    private List<PaxosNodeWrapper> createVirtualNetwork(int totalNbNodes, Network network) {

        List<List<RemotePaxosNode>> networkViews = new ArrayList<>();
        for (int i = 0; i < totalNbNodes; ++i) {
            networkViews.add(new ArrayList<>());
        }

        int nodeId = 0;
        List<PaxosNodeWrapper> paxosNodes = new ArrayList<>();
        for (List<RemotePaxosNode> networkView : networkViews) {
            paxosNodes.add(new PaxosNodeWrapper(new PaxosNode(nodeId, networkView)));
            nodeId++;
        }

        Iterator<PaxosNodeWrapper> refNodeIt = paxosNodes.iterator();
        for (List<RemotePaxosNode> networkView : networkViews) {
            PaxosNodeWrapper refNode = refNodeIt.next();
            for (PaxosNodeWrapper connectedNode : paxosNodes) {
                networkView.add(new NodeConnection(connectedNode, refNode.getAddress(), network));
            }
        }

        for (VirtualNode node : paxosNodes) {
            network.addNode(node);
        }
        network.startAll();
        return paxosNodes;
    }

    public void testSimpleTwoProposals() {
        Network network = new Network();
        List<PaxosNodeWrapper> nodes = createVirtualNetwork(2, network);
        PaxosNode node0 = nodes.get(0).getPaxosNode();
        assert node0.propose("ONE");
        assert !node0.propose("TWO");
    }
}
