package com.lewisesteban.paxos;

import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.List;

import static com.lewisesteban.paxos.NetworkFactory.initSimpleNetwork;

public class PaxosTest extends TestCase {

    public void testTwoProposals() throws IOException {
        List<PaxosNetworkNode> nodes = initSimpleNetwork(2, new Network());
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        assert node0.propose(0, "ONE");
        assert !node0.propose(0, "TWO");
    }

    public void testSameProposals() throws IOException {
        List<PaxosNetworkNode> nodes = initSimpleNetwork(2, new Network());
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        assert node0.propose(0, "ONE");
        assert node0.propose(0, "ONE");
    }

    public void testTwoInstances() throws IOException {
        List<PaxosNetworkNode> nodes = initSimpleNetwork(2, new Network());
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        assert node0.propose(0, "ONE");
        assert !node0.propose(0, "TWO");
        assert node0.propose(1, "TWO");
        assert !node0.propose(1, "ONE");
    }

    public void testMajority() throws IOException {
        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(3, 2, network);
        network.disconnectRack(1);

        int rack0NodeNb = 0;
        int rack1NodeNb = 0;
        PaxosNetworkNode aRack0Node = null;
        PaxosNetworkNode aRack1Node = null;
        for (PaxosNetworkNode node : nodes) {
            if (node.getRack() == 0) {
                rack0NodeNb++;
                aRack0Node = node;
            } else {
                rack1NodeNb++;
                aRack1Node = node;
            }
        }
        assert aRack0Node != null && aRack1Node != null;

        boolean rack0ShouldSucceed = rack0NodeNb > rack1NodeNb;
        boolean rack0Proposal = aRack0Node.getPaxosSrv().propose(0, "DATA");
        boolean rack1Proposal = aRack1Node.getPaxosSrv().propose(0, "DATA");
        if (rack0ShouldSucceed) {
            assert  rack0Proposal && !rack1Proposal;
        } else {
            assert !rack0Proposal && rack1Proposal;
        }
    }

}
