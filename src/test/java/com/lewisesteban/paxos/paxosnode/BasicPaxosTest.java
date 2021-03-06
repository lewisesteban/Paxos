package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.NetworkFactory;
import com.lewisesteban.paxos.PaxosTestCase;
import com.lewisesteban.paxos.client.BasicTestClient;
import com.lewisesteban.paxos.client.CommandException;
import com.lewisesteban.paxos.client.PaxosClient;
import com.lewisesteban.paxos.paxosnode.membership.Membership;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.storage.virtual.InterruptibleVirtualFileAccessor;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.lewisesteban.paxos.NetworkFactory.*;

/**
 * Should be executed with no dedicated proposer.
 */
public class BasicPaxosTest extends PaxosTestCase {

    @Override
    protected void setUp() {
        Membership.LEADER_ELECTION = false;
    }

    public void testTwoProposals() throws IOException {
        final int NB_NODES = 2;
        final AtomicInteger receivedCorrectData = new AtomicInteger(0);
        StateMachine stateMachine = new BasicStateMachine() {
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
        try {
            Thread.sleep(100); // wait for scatter to finish
        } catch (InterruptedException ignored) { }
        assertEquals(NB_NODES, receivedCorrectData.get());
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
            assertTrue(rack0Proposal == Result.CONSENSUS_ON_THIS_CMD && rack1Proposal == Result.NETWORK_ERROR);
        } else {
            assertTrue(rack0Proposal == Result.NETWORK_ERROR && rack1Proposal == Result.CONSENSUS_ON_THIS_CMD);
        }
    }

    public void testSingleRequestParallelism() throws IOException, InterruptedException {
        Network networkA = new Network();
        List<PaxosNetworkNode> nodesA = initSimpleNetwork(10, networkA, stateMachinesEmpty(10));
        networkA.setWaitTimes(30, 40, 40, 0);
        long startTime = System.currentTimeMillis();
        PaxosProposer proposer = nodesA.get(0).getPaxosSrv();
        proposer.propose(cmd1, proposer.getNewInstanceId());
        long timeA = System.currentTimeMillis() - startTime;

        // wait until everyone got cmd
        Thread.sleep(100);
        super.cleanup();

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
        final int NB_REQUESTS = 10;

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
                        // side note: sometimes NoOp will win over a regular cmd on the same instance
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
        assertTrue(parallelTime < (sequentialTime / (NB_CLIENTS / 2)));
    }


    public void testNoWaitForSlowNode() throws IOException {
        Network network = new Network();
        network.setWaitTimes(2, 3, 2000, 0.1f);
        List<PaxosNetworkNode> nodes = initSimpleNetwork(100, new Network(), stateMachinesEmpty(100));
        long startTime = System.currentTimeMillis();
        PaxosProposer proposer = nodes.get(0).getPaxosSrv();
        assertEquals(proposer.propose(cmd1, proposer.getNewInstanceId()).getStatus(), Result.CONSENSUS_ON_THIS_CMD);
        assertTrue(System.currentTimeMillis() - startTime < 2000);
    }

    public void testReturnValue() {
        List<PaxosNetworkNode> nodes = initSimpleNetwork(3, new Network(), stateMachinesAppendOK(3));
        try {
            PaxosProposer proposer = nodes.get(0).getPaxosSrv();
            java.io.Serializable result = proposer.propose(new Command("hi", "", 1), proposer.getNewInstanceId()).getReturnData();
            assertEquals("hiOK", result);
        } catch (IOException e) {
            fail();
        }
    }

    public void testAcceptorStorage() throws IOException {
        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(1, network, stateMachinesIncrement(1));
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        long inst = node0.getNewInstanceId();
        Result res1 = node0.propose(new Command(10, "", 1), inst);
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, res1.getStatus());
        assertEquals(11, res1.getReturnData());
        network.killAll();
        network.startAll();
        Result res2 = node0.propose(new Command(20, "", 2), inst);
        assertEquals(Result.CONSENSUS_ON_ANOTHER_CMD, res2.getStatus());
        assertEquals(11, res2.getReturnData());
    }

    public void testForwardCatchingUp() throws IOException, InterruptedException {
        AtomicInteger receivedAt0 = new AtomicInteger(0);
        Callable<StateMachine> stateMachineCreator0 = basicStateMachine(data -> {
            receivedAt0.incrementAndGet();
            return null;
        });
        AtomicInteger receivedAt1 = new AtomicInteger(0);
        Callable<StateMachine> stateMachineCreator1 = basicStateMachine(data -> {
            receivedAt1.incrementAndGet();
            return null;
        });
        List<Callable<StateMachine>> stateMachines = new ArrayList<>();
        stateMachines.add(stateMachineCreator0);
        stateMachines.add(stateMachineCreator1);
        stateMachines.add(basicStateMachine((Serializable s) -> null));

        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(3, network, stateMachines);
        network.kill(addr(1));
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        for (int i = 0; i < 10; ++i) {
            assertEquals(Result.CONSENSUS_ON_THIS_CMD, node0.propose(new Command(i, "", i), i).getStatus());
        }
        assertEquals(10, receivedAt0.get());
        assertEquals(0, receivedAt1.get());

        System.out.println("+++++ start node 1");
        network.start(addr(1));
        PaxosServer node1 = nodes.get(1).getPaxosSrv();
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, node1.propose(new Command(0, "", 0), 0).getStatus());
        assertEquals(Result.CONSENSUS_ON_ANOTHER_CMD, node1.propose(new Command(0, "", 0), 1).getStatus());
        BasicTestClient basicPaxosClient = new BasicTestClient(node1, "client");
        basicPaxosClient.doCommand("hi");
        assertEquals(11, node1.getNewInstanceId());
        assertEquals(11, receivedAt1.get());

        Thread.sleep(100); // wait for node 1's scatter to reach node 0
        assertEquals(11, receivedAt0.get());
    }

    public void testBackwardCatchingUp() throws IOException {
        AtomicBoolean error = new AtomicBoolean(false);
        AtomicInteger receivedAt0 = new AtomicInteger(0);
        Callable<StateMachine> stateMachineCreator0 = basicStateMachine(data -> {
            if (receivedAt0.getAndIncrement() != (int)data)
                error.set(true);
            return null;
        });
        AtomicInteger receivedAt1 = new AtomicInteger(0);
        Callable<StateMachine> stateMachineCreator1 = basicStateMachine(data -> {
            if (receivedAt1.getAndIncrement() != (int)data)
                error.set(true);
            return null;
        });
        List<Callable<StateMachine>> stateMachines = new ArrayList<>();
        stateMachines.add(stateMachineCreator0);
        stateMachines.add(stateMachineCreator1);
        stateMachines.add(basicStateMachine((Serializable s) -> null));

        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(3, network, stateMachines);
        network.kill(addr(1));
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        for (int i = 0; i < 10; ++i) {
            assertEquals(Result.CONSENSUS_ON_THIS_CMD, node0.propose(new Command(i, "", i), i).getStatus());
        }
        assertEquals(10, receivedAt0.get());
        assertEquals(0, receivedAt1.get());

        System.out.println("+++++ start node 1");
        network.start(addr(1));
        PaxosServer node1 = nodes.get(1).getPaxosSrv();
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, node1.propose(new Command(10, "node1", 0), 10).getStatus());
        assertEquals(11, receivedAt1.get());
        assertEquals(11, node1.getNewInstanceId());
        assertFalse(error.get());
    }

    public void testOrderingSlowCommand() throws InterruptedException {
        Serializable cmd1 = "1";
        Serializable cmd2 = "2";
        AtomicReference<Exception> error = new AtomicReference<>(null);
        Callable<StateMachine> stateMachine = () -> new BasicStateMachine() {
            boolean finishedCmd1 = false;
            @Override
            public Serializable execute(Serializable data) {
                if (data.equals(cmd1)) {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    finishedCmd1 = true;
                } else if (data.equals(cmd2)) {
                    if (!finishedCmd1) {
                        error.set(new Exception("received cmd2 before cmd1"));
                    }
                } else {
                    error.set(new Exception("wrong command: " + data));
                }
                return null;
            }
        };

        List<PaxosNetworkNode> nodes = initSimpleNetwork(1, new Network(), stateMachinesSingle(stateMachine, 1));
        PaxosProposer proposer = nodes.get(0).getPaxosSrv();
        Thread thread1 = new Thread(() -> {
            try {
                proposer.propose(new Command(cmd1, "", 1), 0);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        Thread thread2 = new Thread(() -> {
            try {
                proposer.propose(new Command(cmd2, "", 2), 1);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();
        if (error.get() != null) {
            error.get().printStackTrace();
            fail();
        }
    }

    public void testNoOpCmd() throws IOException {
        AtomicBoolean error = new AtomicBoolean(false);
        Callable<StateMachine> stateMachine = () -> new BasicStateMachine() {
            int receivedCmds = 0;
            @Override
            public Serializable execute(Serializable data) {
                if (receivedCmds == 0) {
                    if (!data.equals(cmd1.getData())) {
                        System.err.println("received " + data + " instead of " + cmd1.getData());
                        error.set(true);
                    } else {
                        receivedCmds = 1;
                    }
                } else if (receivedCmds == 1) {
                    if (!data.equals(cmd2.getData())) {
                        System.err.println("received " + data + " instead of " + cmd2.getData());
                        error.set(true);
                    } else {
                        receivedCmds = 2;
                    }
                } else {
                    System.err.println("received one extra command: " + data);
                    error.set(true);
                }
                return null;
            }
        };

        List<PaxosNetworkNode> nodes = initSimpleNetwork(3, new Network(), stateMachinesSingle(stateMachine, 3));
        PaxosProposer proposer = nodes.get(0).getPaxosSrv();

        Result res1 = proposer.propose(cmd1, 0);
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, res1.getStatus());
        assertEquals(0, res1.getInstanceId());
        Result res2 =  proposer.propose(cmd2, 2);
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, res2.getStatus());
        assertEquals(2, res2.getInstanceId());
        assertFalse(error.get());
    }

    public void testFragmentation() throws CommandException, Exception {
        Network network = new Network();
        List<PaxosNetworkNode> cluster0 = NetworkFactory.initSimpleNetwork(3, network, stateMachinesMirror(3), 0);
        List<PaxosNetworkNode> cluster1 = NetworkFactory.initSimpleNetwork(2, network, stateMachinesMirror(2), 1);

        List<PaxosNetworkNode> wholeNet = new ArrayList<>();
        wholeNet.addAll(cluster0);
        wholeNet.addAll(cluster1);
        List<PaxosServer> nodes = wholeNet.stream().map(PaxosNetworkNode::getPaxosSrv).collect(Collectors.toList());
        PaxosClient<PaxosServer> client = new PaxosClient<>(nodes, "client", InterruptibleVirtualFileAccessor.storageUnitCreator(-1));
        client.tryCommand("1", 0);
        client.tryCommand("2", 0);
        client.tryCommand("3", 1);

        Thread.sleep(100); // wait for scatter to finish, otherwise "getNewInstanceId" might return an old inst
        PaxosProposer proposer0 = cluster0.get(0).getPaxosSrv();
        assertEquals(2, proposer0.getNewInstanceId());
        assertEquals("2", proposer0.propose(cmd4, 1).getReturnData());
        PaxosProposer proposer1 = cluster1.get(0).getPaxosSrv();
        assertEquals(1, proposer1.getNewInstanceId());
        assertEquals("3", proposer1.propose(cmd4, 0).getReturnData());
    }
}
