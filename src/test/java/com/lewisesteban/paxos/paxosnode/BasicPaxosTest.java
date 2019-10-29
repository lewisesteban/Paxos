package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.PaxosTestCase;
import com.lewisesteban.paxos.client.BasicPaxosClient;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.listener.SnapshotManager;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.storage.FileAccessor;
import com.lewisesteban.paxos.storage.StorageException;
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

import static com.lewisesteban.paxos.NetworkFactory.*;

/**
 * Should be executed with no dedicated proposer.
 */
public class BasicPaxosTest extends PaxosTestCase {

    private static final Command cmd1 = new Command("ONE", "", 1);
    private static final Command cmd2 = new Command("TWO", "", 2);

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

    public void testBasicCatchingUp() throws IOException, InterruptedException {
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
        network.kill(1);
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        for (int i = 0; i < 10; ++i) {
            assertEquals(Result.CONSENSUS_ON_THIS_CMD, node0.propose(new Command(i, "", i), i).getStatus());
        }
        assertEquals(10, receivedAt0.get());
        assertEquals(0, receivedAt1.get());

        System.out.println("+++++ start node 1");
        network.start(1);
        PaxosServer node1 = nodes.get(1).getPaxosSrv();
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, node1.propose(new Command(0, "", 0), 0).getStatus());
        assertEquals(Result.CONSENSUS_ON_ANOTHER_CMD, node1.propose(new Command(0, "", 0), 1).getStatus());
        BasicPaxosClient basicPaxosClient = new BasicPaxosClient(node1, "client");
        basicPaxosClient.doCommand("hi");
        assertEquals(11, node1.getNewInstanceId());
        assertEquals(11, receivedAt1.get());

        Thread.sleep(100); // wait for node 1's scatter to reach node 0
        assertEquals(11, receivedAt0.get());
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

    public void testLogRemovalAfterSnapshot() throws IOException {
        Callable<StateMachine> stateMachine = basicStateMachine((val) -> null);
        List<PaxosNetworkNode> nodes = initSimpleNetwork(1, new Network(), stateMachinesSingle(stateMachine, 1));
        PaxosProposer proposer = nodes.get(0).getPaxosSrv();
        SnapshotManager.SNAPSHOT_FREQUENCY = 3;
        SnapshotManager.KEEP_AFTER_SNAPSHOT = 1;
        proposer.propose(new Command("0", "client", 0), proposer.getNewInstanceId());
        proposer.propose(new Command("1", "client", 1), proposer.getNewInstanceId());
        assertEquals(2, InterruptibleVirtualFileAccessor.creator(0).create("acceptor0", null).listFiles().length);
        proposer.propose(new Command("2", "client", 2), proposer.getNewInstanceId());
        assertEquals(SnapshotManager.KEEP_AFTER_SNAPSHOT, InterruptibleVirtualFileAccessor.creator(0).create("acceptor0", null).listFiles().length);
    }

    public void testDownloadSnapshot() throws IOException {
        AtomicBoolean snapshotLoaded = new AtomicBoolean(false);
        AtomicInteger snapshotCreated = new AtomicInteger(0);
        AtomicReference<String> stateMachineError = new AtomicReference<>(null);
        Callable<StateMachine> stateMachine = () -> new BasicStateMachine() {
            @Override
            public Serializable execute(Serializable data) {
                return data;
            }

            @Override
            public void loadSnapshot(Snapshot snapshot) {
                super.loadSnapshot(snapshot);
                if (getNodeId() != 2)
                    stateMachineError.set("snapshot loaded on wrong server");
                else {
                    if (snapshotLoaded.get())
                        stateMachineError.set("snapshot loaded more than once");
                    else
                        snapshotLoaded.set(true);
                }
            }

            @Override
            public void snapshot(long lastSnapshottedInstance) throws StorageException {
                super.snapshot(lastSnapshottedInstance);
                if (getNodeId() == 2)
                    stateMachineError.set("snapshot created on node 2");
                else
                    snapshotCreated.incrementAndGet();
            }
        };

        // init network
        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(3, network, stateMachinesSame(stateMachine, 3));
        PaxosProposer proposer = nodes.get(0).getPaxosSrv();
        PaxosServer lateServer = nodes.get(2).getPaxosSrv();
        network.kill(2);

        // do three commands with server 2 down
        SnapshotManager.SNAPSHOT_FREQUENCY = 3;
        SnapshotManager.KEEP_AFTER_SNAPSHOT = 1;
        proposer.propose(new Command("0", "client", 0), 0);
        proposer.propose(new Command("1", "client", 1), 1);
        proposer.propose(new Command("2", "client", 2), 2);
        FileAccessor srv2Dir = InterruptibleVirtualFileAccessor.creator(2).create("acceptor2", null);
        assertTrue(!srv2Dir.exists() || srv2Dir.listFiles() == null || srv2Dir.listFiles().length == 0);

        // do requests on failed server
        network.start(2);
        Result result;
        result = lateServer.propose(new Command("3", "anotherClient", 0), lateServer.getNewInstanceId());
        assertEquals(Result.CONSENSUS_ON_ANOTHER_CMD, result.getStatus());
        long newInst = lateServer.getNewInstanceId();
        assertEquals(3, newInst);
        result = lateServer.propose(new Command("3", "anotherClient", 0), newInst);
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, result.getStatus());
        assertEquals(3, result.getInstanceId());
        assertEquals("3", result.getReturnData());

        // check  files
        FileAccessor[] files = InterruptibleVirtualFileAccessor.creator(2).create("acceptor2", null).listFiles();
        assertTrue(files.length == 1 || files.length == 2); // note: there might be an extra file left that is within the snapshot (due to the fact that the acceptor is not synchronized with the snapshotting process), but the acceptor's InstanceManager will not use the information contained in it
        files = InterruptibleVirtualFileAccessor.creator(1).create("acceptor1", null).listFiles();
        assertEquals(2, files.length);
        if (!(files[0].getName().equals("inst2") || files[1].getName().equals("inst2")))
            fail();
        if (!(files[0].getName().equals("inst3") || files[1].getName().equals("inst3")))
            fail();

        // check state machine
        if (stateMachineError.get() != null) {
            System.out.println(stateMachineError.get());
            fail();
        }
        if (!snapshotLoaded.get())
            fail();
        if (snapshotCreated.get() != 2)
            fail();
    }

    public void testRecoveryAfterSnapshot() throws IOException {
        List<Serializable> stateMachineReceived = new ArrayList<>();
        Callable<StateMachine> stateMachine = basicStateMachine((data) -> {
            stateMachineReceived.add(data);
            return data;
        });
        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(1, network, stateMachinesSame(stateMachine, 1));
        PaxosServer server = nodes.get(0).getPaxosSrv();
        SnapshotManager.SNAPSHOT_FREQUENCY = 3;
        SnapshotManager.KEEP_AFTER_SNAPSHOT = 1;

        // do three proposals
        Command cmd2 = new Command("2", "client", 2);
        server.propose(new Command("0", "client", 0), 0);
        server.propose(new Command("1", "client", 1), 1);
        server.propose(cmd2, 2);
        network.kill(0);
        network.start(0);

        // check recovery
        Result result;
        result = server.propose(new Command("3", "client", 3), 2);
        assertEquals(Result.CONSENSUS_ON_ANOTHER_CMD, result.getStatus());
        assertEquals(2, result.getInstanceId());
        PrepareAnswer prepareAnswer = server.getAcceptor().reqPrepare(2, new Proposal.ID(0, 10000));
        assertTrue(prepareAnswer.isPrepareOK());
        assertEquals(prepareAnswer.getAlreadyAccepted().getCommand(), cmd2);
        assertFalse(prepareAnswer.isSnapshotRequestRequired());

        // try another proposal
        result = server.propose(new Command("3", "client", 3), server.getNewInstanceId());
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, result.getStatus());
        assertEquals(3, result.getInstanceId());
        assertEquals("3", result.getReturnData());

        // check state machine
        assertEquals(4, stateMachineReceived.size());
        assertEquals("0", stateMachineReceived.get(0));
        assertEquals("1", stateMachineReceived.get(1));
        assertEquals("2", stateMachineReceived.get(2));
        assertEquals("3", stateMachineReceived.get(3));
    }

    // TODO test slow snapshot responsiveness
}
