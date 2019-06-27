package com.lewisesteban.paxos;

import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.lewisesteban.paxos.NetworkFactory.*;

/**
 * Should be executed with no dedicated proposer.
 */
public class BasicPaxosTest extends TestCase {

    private static final Command cmd1 = new Command(0, 1, "ONE");
    private static final Command cmd2 = new Command(0, 2, "TWO");

    public void testTwoProposals() throws IOException {
        final int NB_NODES = 2;
        final AtomicInteger receivedCorrectData = new AtomicInteger(0);
        Executor executor = (i, command) -> {
            if (command.getData().equals("ONE") && i == 0) {
                receivedCorrectData.incrementAndGet();
            } else {
                receivedCorrectData.set(-NB_NODES);
                System.err.println("INCORRECT: instance= " + i + " data=" + command.getData().toString());
            }
        };
        List<PaxosNetworkNode> nodes = initSimpleNetwork(NB_NODES, new Network(), executorsSame(executor, NB_NODES));
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        assertTrue(node0.propose(cmd1, 0).getSuccess());
        assertFalse(node0.propose(cmd2, 0).getSuccess());
        assertTrue(receivedCorrectData.get() >= NB_NODES);
    }

    public void testSameProposals() throws IOException {
        List<PaxosNetworkNode> nodes = initSimpleNetwork(2, new Network(), executorsEmpty(2));
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        assertTrue(node0.propose(cmd1, 0).getSuccess());
        assertTrue(node0.propose(cmd1, 0).getSuccess());
    }

    public void testTwoInstances() throws IOException {
        List<PaxosNetworkNode> nodes = initSimpleNetwork(2, new Network(), executorsEmpty(2));
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        assertTrue(node0.propose(cmd1, 0).getSuccess());
        assertFalse(node0.propose(cmd2, 0).getSuccess());
        assertTrue(node0.propose(cmd2, 1).getSuccess());
        assertFalse(node0.propose(cmd1, 1).getSuccess());
    }

    public void testMajority() throws IOException {
        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(3, 2, network, executorsEmpty(3));
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
        boolean rack0Proposal = aRack0Node.getPaxosSrv().propose(cmd1, 0).getSuccess();
        boolean rack1Proposal = aRack1Node.getPaxosSrv().propose(cmd2, 0).getSuccess();
        if (rack0ShouldSucceed) {
            assertTrue(rack0Proposal && !rack1Proposal);
        } else {
            assertTrue(!rack0Proposal && rack1Proposal);
        }
    }

    public void testSingleRequestParallelism() throws IOException {
        Network networkA = new Network();
        List<PaxosNetworkNode> nodesA = initSimpleNetwork(10, networkA, executorsEmpty(10));
        networkA.setWaitTimes(30, 40, 40, 0);
        long startTime = System.currentTimeMillis();
        nodesA.get(0).getPaxosSrv().proposeNew(cmd1);
        long timeA = System.currentTimeMillis() - startTime;

        Network networkB = new Network();
        List<PaxosNetworkNode> nodesB = initSimpleNetwork(100, networkB, executorsEmpty(100));
        networkB.setWaitTimes(30, 40, 40, 0);
        startTime = System.currentTimeMillis();
        nodesB.get(0).getPaxosSrv().proposeNew(cmd2);
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
        List<PaxosNetworkNode> nodes = initSimpleNetwork(NB_NODES, network, executorsEmpty(NB_NODES));
        PaxosServer dedicatedServer = nodes.get(0).getPaxosSrv();

        Thread seqClient = new Thread(() -> {
            for (int cmdId = 0; cmdId < NB_REQUESTS * NB_CLIENTS; cmdId++) {
                try {
                    dedicatedServer.proposeNew(cmd1);
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
                        dedicatedServer.proposeNew(cmd1);
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
        List<PaxosNetworkNode> nodes = initSimpleNetwork(100, new Network(), executorsEmpty(100));
        long startTime = System.currentTimeMillis();
        assertTrue(nodes.get(0).getPaxosSrv().proposeNew(cmd1).getSuccess());
        assertTrue(System.currentTimeMillis() - startTime < 1000);
    }

}
