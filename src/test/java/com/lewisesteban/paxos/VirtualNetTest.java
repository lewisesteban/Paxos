package com.lewisesteban.paxos;

import com.lewisesteban.paxos.client.BasicPaxosClient;
import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.PaxosNode;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.storage.FileAccessorCreator;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.lewisesteban.paxos.NetworkFactory.*;
import static com.lewisesteban.paxos.virtualnet.server.PaxosServer.SRV_FAILURE_MSG;

public class VirtualNetTest extends PaxosTestCase {

    private static final Command cmd1 = new Command("ONE", "", 1);

    private boolean slowPropose = false;
    private int slowAcceptorId = 0;

    public void testCutRackConnection() throws IOException {
        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(2, 2, network, stateMachinesEmpty(2));
        network.disconnectRack(1);
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        assertEquals(node0.propose(cmd1, 0).getStatus(), Result.NETWORK_ERROR);
    }

    public void testSlowNetworkDown() throws IOException {
        final int NB_TESTS = 10;
        final int DIFF = 5;

        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(2, network, stateMachinesEmpty(2));
        PaxosServer node0 = nodes.get(0).getPaxosSrv();

        network.setWaitTimes(0, 1, 1, 0);
        long start = System.currentTimeMillis();
        for (int i = 0; i < NB_TESTS; i++) {
            node0.propose(Command.Factory.makeRandom("fast network"), i);
        }
        long time1 = System.currentTimeMillis() - start;

        network.setWaitTimes(5, 6, 10000, 0);
        start = System.currentTimeMillis();
        for (int i = 0; i < NB_TESTS; i++) {
            node0.propose(Command.Factory.makeRandom("slow network test"), NB_TESTS + i);
        }
        long time2 = System.currentTimeMillis() - start;
        assertTrue((time2 - time1 > ((DIFF - 1) * NB_TESTS * 2)) && (time2 - time1 < 10000));

        network.setWaitTimes(0, 1, DIFF * 2, 0.6f);
        start = System.currentTimeMillis();
        for (int i = 0; i < NB_TESTS; i++) {
            node0.propose(Command.Factory.makeRandom("unusual wait test"), (2 * NB_TESTS) + i);
        }
        long time3 = System.currentTimeMillis() - start;
        assertTrue((time3 - time1 > ((DIFF - 1) * NB_TESTS)));
    }

    public void testKillProposingServer() throws InterruptedException {

        final int NB_CLIENTS = 5;
        final AtomicInteger nbSuccesses = new AtomicInteger(0);
        final AtomicInteger nbCorrectExceptions = new AtomicInteger(0);

        Network network = new Network();
        List<PaxosNetworkNode> nodes = NetworkFactory.initSimpleNetwork(2, network, SlowPaxosNode::new, stateMachinesEmpty(2));
        final PaxosNetworkNode server = nodes.get(0);

        slowPropose = true;
        List<Thread> clients = new ArrayList<>();
        for (int i = 0; i < NB_CLIENTS; ++i) {
            final String proposalData = String.valueOf(i);
            clients.add(new Thread(() -> {
                try {
                    server.getPaxosSrv().propose(new Command(proposalData, "", 1), 0);
                    nbSuccesses.incrementAndGet();
                } catch (IOException e) {
                    if (e.getMessage().equals(SRV_FAILURE_MSG) && !(e.getCause() instanceof ExecutionException))
                        nbCorrectExceptions.getAndIncrement();
                    System.out.println("Got exception " + e.getMessage());
                }
            }));
        }
        for (Thread client : clients) {
            client.start();
        }
        System.out.println("Threads started. Let's wait a bit...");
        Thread.sleep(100); // wait for the proposeNew/accept tasks to start
        System.out.println("Enough waiting. Kill the servers.");

        network.kill(0);
        for (Thread client : clients) {
            try {
                client.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        assertEquals(0, nbSuccesses.get());
        assertEquals(NB_CLIENTS, nbCorrectExceptions.get());
    }

    public void testKillAcceptingServer() throws InterruptedException, ExecutionException {

        Network network = new Network();
        List<PaxosNetworkNode> nodes = NetworkFactory.initSimpleNetwork(2, network, SlowPaxosNode::new, stateMachinesEmpty(2));
        slowAcceptorId = 1;
        final PaxosNetworkNode server = nodes.get(0);

        slowPropose = false;
        FutureTask<Byte> client = new FutureTask<>(() -> server.getPaxosSrv().propose(cmd1, 0).getStatus());
        new Thread(client).start();

        System.out.println("Task started. Let's wait a bit...");
        Thread.sleep(100); // wait for the proposeNew/accept tasks to start
        System.out.println("Enough waiting. Kill the server.");
        network.kill(slowAcceptorId);
        assertEquals((byte) client.get(), Result.NETWORK_ERROR);
    }

    public void testKillAndRestartSingleServer() {
        List<PaxosNetworkNode> nodes = initSimpleNetwork(1, new Network(), Collections.nCopies(1, () -> new StateMachine() {
            int nbCommandsReceived = 0;
            @Override
            public java.io.Serializable execute(java.io.Serializable data) {
                nbCommandsReceived++;
                return nbCommandsReceived;
            }
        }));
        try {
            Command cmd = new Command("hi", "", 1);
            PaxosProposer proposer = nodes.get(0).getPaxosSrv();
            proposer.propose(cmd, proposer.getNewInstanceId());
            nodes.get(0).kill();
            nodes.get(0).start();
            proposer.propose(cmd, proposer.getNewInstanceId());
            Result result = nodes.get(0).getPaxosSrv().propose(cmd, proposer.getNewInstanceId());
            assertEquals(2, result.getReturnData());
        } catch (IOException e) {
            fail();
        }
    }

    public void testKillAndRestartSlowAcceptor() throws InterruptedException {

        Network network = new Network();
        List<PaxosNetworkNode> nodes = NetworkFactory.initSimpleNetwork(2, network, SlowPaxosNode::new, stateMachinesAppendOK(2));
        slowAcceptorId = 1;
        final PaxosNetworkNode server = nodes.get(0);

        AtomicBoolean success = new AtomicBoolean(false);
        AtomicBoolean serverRestarted = new AtomicBoolean(false);
        slowPropose = false;
        BasicPaxosClient client = new BasicPaxosClient(server.getPaxosSrv(), "");
        Thread thread = new Thread(() -> {
            try {
                Serializable res = client.tryDoCommand("hi");
                if (!serverRestarted.get()) {
                    System.err.println("too early");
                    success.set(false);
                } else {
                    if (res.equals("hiOK")) {
                        success.set(true);
                    } else {
                        success.set(false);
                        System.err.println("wrong res: " + res);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                success.set(false);
            }
        });
        thread.start();

        System.out.println("Task started. Let's wait a bit...");
        Thread.sleep(100); // wait for the proposeNew/accept tasks to start
        System.out.println("Enough waiting. Kill the server.");
        network.kill(slowAcceptorId);
        int killedSrvId = slowAcceptorId;

        slowAcceptorId = -1; // now no acceptor will be slow
        System.out.println("Now start again.");
        serverRestarted.set(true);
        network.start(killedSrvId);

        thread.join();
        assertTrue(success.get());
    }

    private class SlowPaxosNode extends PaxosNode {

        private AcceptorRPCHandle acceptor;

        SlowPaxosNode(int id, List<RemotePaxosNode> remotePaxosNodeList, StateMachine stateMachine, StorageUnit.Creator storage, FileAccessorCreator fileAccessorCreator) throws StorageException {
            super(id, remotePaxosNodeList, stateMachine, storage, fileAccessorCreator);
            acceptor = new SlowPaxosAcceptor(super.getAcceptor());
        }

        @Override
        public Result propose(Command command, long inst) throws IOException {
            if (slowPropose) {
                doHeavyWork();
            }
            return super.propose(command, inst);
        }

        public AcceptorRPCHandle getAcceptor() {
            return acceptor;
        }

        class SlowPaxosAcceptor implements AcceptorRPCHandle {

            AcceptorRPCHandle acceptor;

            SlowPaxosAcceptor(AcceptorRPCHandle acceptor) {
                this.acceptor = acceptor;
            }

            @Override
            public PrepareAnswer reqPrepare(long instanceId, Proposal.ID propId) throws IOException {
                if (getId() == slowAcceptorId) {
                    doHeavyWork();
                }
                return acceptor.reqPrepare(instanceId, propId);
            }

            @Override
            public boolean reqAccept(long instanceId, Proposal proposal) throws IOException {
                return acceptor.reqAccept(instanceId, proposal);
            }

            @Override
            public long getLastInstance() throws IOException {
                return acceptor.getLastInstance();
            }
        }

        void doHeavyWork() {
            @SuppressWarnings("MismatchedQueryAndUpdateOfStringBuilder")
            StringBuilder str = new StringBuilder();
            for (int i = 0; i < 100000000; ++i) {
                if (i % 1000 == 0) {
                    str = new StringBuilder();
                }
                str.append(i);
            }
        }
    }
}
