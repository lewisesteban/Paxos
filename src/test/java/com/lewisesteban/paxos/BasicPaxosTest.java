package com.lewisesteban.paxos;

import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.lewisesteban.paxos.NetworkFactory.initSimpleNetwork;

public class BasicPaxosTest extends TestCase {

    private final Executor noExec = (i, d) -> {};

    public void testTwoProposals() throws IOException {
        final int NB_NODES = 2;
        final AtomicInteger receivedCorrectData = new AtomicInteger(0);
        Executor executor = (i, data) -> {
            if (data.equals("ONE") && i == 0) {
                receivedCorrectData.incrementAndGet();
            } else {
                receivedCorrectData.set(-NB_NODES);
                System.err.println("INCORRECT: instance= " + i + " data=" + data.toString());
            }
        };
        List<PaxosNetworkNode> nodes = initSimpleNetwork(NB_NODES, new Network(), executor);
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        assertTrue(node0.propose("ONE", 0).getSuccess());
        assertFalse(node0.propose("TWO", 0).getSuccess());
        assertTrue(receivedCorrectData.get() >= NB_NODES);
    }

    public void testSameProposals() throws IOException {
        List<PaxosNetworkNode> nodes = initSimpleNetwork(2, new Network(), noExec);
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        assertTrue(node0.propose("ONE", 0).getSuccess());
        assertTrue(node0.propose("ONE", 0).getSuccess());
    }

    public void testTwoInstances() throws IOException {
        List<PaxosNetworkNode> nodes = initSimpleNetwork(2, new Network(), noExec);
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        assertTrue(node0.propose("ONE", 0).getSuccess());
        assertFalse(node0.propose("TWO", 0).getSuccess());
        assertTrue(node0.propose("TWO", 1).getSuccess());
        assertFalse(node0.propose("ONE", 1).getSuccess());
    }

    public void testMajority() throws IOException {
        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(3, 2, network, noExec);
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
        assertTrue(aRack0Node != null && aRack1Node != null);

        boolean rack0ShouldSucceed = rack0NodeNb > rack1NodeNb;
        boolean rack0Proposal = aRack0Node.getPaxosSrv().propose("DATA", 0).getSuccess();
        boolean rack1Proposal = aRack1Node.getPaxosSrv().propose("DATA", 0).getSuccess();
        if (rack0ShouldSucceed) {
            assertTrue(rack0Proposal && !rack1Proposal);
        } else {
            assertTrue(!rack0Proposal && rack1Proposal);
        }
    }

    public void testParallelism() throws IOException {
        Network networkA = new Network();
        List<PaxosNetworkNode> nodesA = initSimpleNetwork(10, networkA, noExec);
        networkA.setWaitTimes(30, 40, 40, 0);
        long startTime = System.currentTimeMillis();
        nodesA.get(0).getPaxosSrv().propose("VAL", 0);
        long timeA = System.currentTimeMillis() - startTime;

        Network networkB = new Network();
        List<PaxosNetworkNode> nodesB = initSimpleNetwork(100, networkB, noExec);
        networkB.setWaitTimes(30, 40, 40, 0);
        startTime = System.currentTimeMillis();
        nodesB.get(0).getPaxosSrv().propose("VAL", 0);
        long timeB = System.currentTimeMillis() - startTime;

        System.out.println(timeA + " " + timeB);
        assertTrue(timeB < timeA * 2);
    }

    public void testNoWaitForSlowNode() throws IOException {
        Network network = new Network();
        network.setWaitTimes(2, 3, 1000, 0.1f);
        List<PaxosNetworkNode> nodes = initSimpleNetwork(100, new Network(), noExec);
        long startTime = System.currentTimeMillis();
        assertTrue(nodes.get(0).getPaxosSrv().propose("DATA", 0).getSuccess());
        assertTrue(System.currentTimeMillis() - startTime < 1000);
    }

}
