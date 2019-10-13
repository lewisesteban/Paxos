package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;
import junit.framework.TestCase;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.lewisesteban.paxos.NetworkFactory.*;

/**
 * Should be executed with no dedicated proposer.
 */
public class BasicPaxosTest extends TestCase {

    private static final Serializable cmd1 = "ONE";
    private static final Serializable cmd2 = "TWO";

    public void testTwoProposals() throws IOException {
        final int NB_NODES = 2;
        final AtomicInteger receivedCorrectData = new AtomicInteger(0);
        StateMachine stateMachine = new StateMachine() {
            boolean received = false;
            @Override
            public java.io.Serializable execute(java.io.Serializable data) {
                if (!received && data.equals("ONE")) {
                    receivedCorrectData.incrementAndGet();
                } else {
                    receivedCorrectData.set(-NB_NODES);
                }
                return null;
            }
        };
        List<PaxosNetworkNode> nodes = initSimpleNetwork(NB_NODES, new Network(), stateMachinesSame(() -> stateMachine, NB_NODES));
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        assertEquals(node0.propose(cmd1, 0).getStatus(), Result.CONSENSUS_ON_THIS_CMD);
        assertEquals(node0.propose(cmd2, 0).getStatus(), Result.CONSENSUS_ON_ANOTHER_CMD);
        assertEquals(receivedCorrectData.get(), NB_NODES);
    }

    public void testSameProposals() throws IOException {
        List<PaxosNetworkNode> nodes = initSimpleNetwork(2, new Network(), stateMachinesEmpty(2));
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        assertEquals(node0.propose(cmd1, 0).getStatus(), Result.CONSENSUS_ON_THIS_CMD);
        assertEquals(node0.propose(cmd1, 0).getStatus(), Result.CONSENSUS_ON_THIS_CMD);
    }

    public void testTwoInstances() throws IOException {
        List<PaxosNetworkNode> nodes = initSimpleNetwork(2, new Network(), stateMachinesEmpty(2));
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        assertEquals(node0.propose(cmd1, 0).getStatus(), Result.CONSENSUS_ON_THIS_CMD);
        assertEquals(node0.propose(cmd2, 0).getStatus(), Result.CONSENSUS_ON_ANOTHER_CMD);
        assertEquals(node0.propose(cmd2, 1).getStatus(), Result.CONSENSUS_ON_THIS_CMD);
        assertEquals(node0.propose(cmd1, 1).getStatus(), Result.CONSENSUS_ON_ANOTHER_CMD);
    }

    public void testMajority() throws IOException {
        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(3, 2, network, stateMachinesEmpty(3));
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
        byte rack0Proposal = aRack0Node.getPaxosSrv().propose(cmd1, 0).getStatus();
        byte rack1Proposal = aRack1Node.getPaxosSrv().propose(cmd2, 0).getStatus();
        if (rack0ShouldSucceed) {
            assertTrue(rack0Proposal == Result.CONSENSUS_ON_THIS_CMD && rack1Proposal == Result.CONSENSUS_FAILED);
        } else {
            assertTrue(rack0Proposal == Result.CONSENSUS_FAILED && rack1Proposal == Result.CONSENSUS_ON_THIS_CMD);
        }
    }

    public void testSingleRequestParallelism() throws IOException {
        Network networkA = new Network();
        List<PaxosNetworkNode> nodesA = initSimpleNetwork(10, networkA, stateMachinesEmpty(10));
        networkA.setWaitTimes(30, 40, 40, 0);
        long startTime = System.currentTimeMillis();
        PaxosProposer proposer = nodesA.get(0).getPaxosSrv();
        proposer.propose(cmd1, proposer.getNewInstanceId());
        long timeA = System.currentTimeMillis() - startTime;

        Network networkB = new Network();
        List<PaxosNetworkNode> nodesB = initSimpleNetwork(100, networkB, stateMachinesEmpty(100));
        networkB.setWaitTimes(30, 40, 40, 0);
        startTime = System.currentTimeMillis();
        proposer = nodesB.get(0).getPaxosSrv();
        proposer.propose(cmd2, proposer.getNewInstanceId());
        long timeB = System.currentTimeMillis() - startTime;

        System.out.println(timeA + " " + timeB);
        assertTrue(timeB < (float)timeA * 2.5);
    }

    public void testClientParallelism() throws Exception {
        final int NB_NODES = 3;
        final int NB_CLIENTS = 4;
        final int NB_REQUESTS = 5;

        Network network = new Network();
        network.setWaitTimes(20, 20, 1, 0);
        List<PaxosNetworkNode> nodes = initSimpleNetwork(NB_NODES, network, stateMachinesEmpty(NB_NODES));
        PaxosServer dedicatedServer = nodes.get(0).getPaxosSrv();

        Thread seqClient = new Thread(() -> {
            for (int cmdId = 0; cmdId < NB_REQUESTS * NB_CLIENTS; cmdId++) {
                try {
                    dedicatedServer.propose(cmd1, dedicatedServer.getNewInstanceId());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        long startTime = System.currentTimeMillis();
        seqClient.start();
        seqClient.join();
        long sequentialTime = System.currentTimeMillis() - startTime;

        Thread[] clients = new Thread[NB_CLIENTS];
        for (int clientId = 0; clientId < NB_CLIENTS; ++clientId) {
            clients[clientId] = new Thread(() -> {
                for (int cmdId = 0; cmdId < NB_REQUESTS; cmdId++) {
                    try {
                        dedicatedServer.propose(cmd1, dedicatedServer.getNewInstanceId());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        startTime = System.currentTimeMillis();
        for (Thread client : clients) {
            client.start();
        }
        for (Thread client : clients) {
            client.join();
        }
        long parallelTime = System.currentTimeMillis() - startTime;

        System.out.println("Sequential: " + sequentialTime);
        System.out.println("Parallel: " + parallelTime);
        assertTrue(parallelTime < (sequentialTime / (NB_CLIENTS - 1)));
    }


    public void testNoWaitForSlowNode() throws IOException {
        Network network = new Network();
        network.setWaitTimes(2, 3, 1000, 0.1f);
        List<PaxosNetworkNode> nodes = initSimpleNetwork(100, new Network(), stateMachinesEmpty(100));
        long startTime = System.currentTimeMillis();
        PaxosProposer proposer = nodes.get(0).getPaxosSrv();
        assertEquals(proposer.propose(cmd1, proposer.getNewInstanceId()).getStatus(), Result.CONSENSUS_ON_THIS_CMD);
        assertTrue(System.currentTimeMillis() - startTime < 1000);
    }

    public void testReturnValue() {
        List<PaxosNetworkNode> nodes = initSimpleNetwork(3, new Network(), stateMachinesAppendOK(3));
        try {
            PaxosProposer proposer = nodes.get(0).getPaxosSrv();
            java.io.Serializable result = proposer.propose("hi", proposer.getNewInstanceId()).getReturnData();
            assertEquals("hiOK", result);
        } catch (IOException e) {
            fail();
        }
    }
}
