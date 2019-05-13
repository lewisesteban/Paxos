package com.lewisesteban.paxos;

import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.List;

import static com.lewisesteban.paxos.NetworkFactory.initSimpleNetwork;

public class PaxosTest extends TestCase {

    public void testSimpleTwoProposals() throws IOException {
        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(2, network);
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        assert node0.propose("ONE");
        assert !node0.propose("TWO");
    }
}
