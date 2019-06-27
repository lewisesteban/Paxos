package com.lewisesteban.paxos;

import com.lewisesteban.paxos.node.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.node.proposer.Proposal;
import com.lewisesteban.paxos.node.proposer.Result;
import com.lewisesteban.paxos.rpc.AcceptorRPCHandle;
import com.lewisesteban.paxos.rpc.RemotePaxosNode;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import static com.lewisesteban.paxos.NetworkFactory.executorsEmpty;
import static com.lewisesteban.paxos.NetworkFactory.initSimpleNetwork;
import static com.lewisesteban.paxos.virtualnet.server.PaxosServer.SRV_FAILURE_MSG;

public class VirtualNetTest extends TestCase {

    private static final Command cmd1 = new Command(0, 1, "ONE");
    private static final Command cmd2 = new Command(0, 2, "TWO");

    private boolean slowPropose = false;
    private int slowAcceptorId = 0;

    public void testCutRackConnection() throws IOException {
        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(2, 2, network, executorsEmpty(2));
        network.disconnectRack(1);
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        assertFalse(node0.propose(cmd1, 0).getSuccess());
    }

    public void testSlowNetworkDown() throws IOException {
        final int NB_TESTS = 10;
        final int DIFF = 5;

        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(2, network, executorsEmpty(2));
        PaxosServer node0 = nodes.get(0).getPaxosSrv();
        assertTrue(node0.propose(cmd1, 0).getSuccess());

        network.setWaitTimes(0, 1, 1, 0);
        long start = System.currentTimeMillis();
        for (int i = 0; i < NB_TESTS; i++) {
            node0.propose(new Command(0, 2, "fast network"), 0);
        }
        long time1 = System.currentTimeMillis() - start;

        network.setWaitTimes(5, 6, 10000, 0);
        start = System.currentTimeMillis();
        for (int i = 0; i < NB_TESTS; i++) {
            node0.propose(new Command(0, 3,"slow network test"), 0);
        }
        long time2 = System.currentTimeMillis() - start;
        assertTrue((time2 - time1 > ((DIFF - 1) * NB_TESTS * 2)) && (time2 - time1 < 10000));

        network.setWaitTimes(0, 1, DIFF * 2, 0.6f);
        start = System.currentTimeMillis();
        for (int i = 0; i < NB_TESTS; i++) {
            node0.propose(new Command(0, 4,"unusual wait test"), 0);
        }
        long time3 = System.currentTimeMillis() - start;
        assertTrue((time3 - time1 > ((DIFF - 1) * NB_TESTS)));
    }

    public void testKillProposingServer() throws InterruptedException {

        final int NB_CLIENTS = 5;
        final AtomicInteger nbSuccesses = new AtomicInteger(0);
        final AtomicInteger nbCorrectExceptions = new AtomicInteger(0);

        Network network = new Network();
        List<PaxosNetworkNode> nodes = NetworkFactory.initSimpleNetwork(2, network, SlowPaxosNode::new, executorsEmpty(2));
        final PaxosNetworkNode server = nodes.get(0);

        slowPropose = true;
        List<Thread> clients = new ArrayList<>();
        for (int i = 0; i < NB_CLIENTS; ++i) {
            final int thisClient = i;
            final String proposalData = String.valueOf(i);
            clients.add(new Thread(() -> {
                try {
                    server.getPaxosSrv().propose(new Command(thisClient, 0, proposalData), 0);
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
        List<PaxosNetworkNode> nodes = NetworkFactory.initSimpleNetwork(2, network, SlowPaxosNode::new, executorsEmpty(2));
        slowAcceptorId = 1;
        final PaxosNetworkNode server = nodes.get(0);

        slowPropose = false;
        FutureTask<Boolean> client = new FutureTask<>(() -> server.getPaxosSrv().propose(cmd1, 0).getSuccess());
        new Thread(client).start();

        System.out.println("Task started. Let's wait a bit...");
        Thread.sleep(100); // wait for the proposeNew/accept tasks to start
        System.out.println("Enough waiting. Kill the servers.");

        network.kill(slowAcceptorId);
        assertFalse(client.get());
    }

    private class SlowPaxosNode extends PaxosNode {

        private AcceptorRPCHandle acceptor;

        SlowPaxosNode(int id, List<RemotePaxosNode> remotePaxosNodeList, Executor executor) {
            super(id, remotePaxosNodeList, executor);
            acceptor = new SlowPaxosAcceptor(getAcceptor());
        }

        @Override
        public Result propose(Command command, int inst) throws IOException {
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
            public PrepareAnswer reqPrepare(int instanceId, Proposal.ID propId) throws IOException {
                if (getId() == slowAcceptorId) {
                    doHeavyWork();
                }
                return acceptor.reqPrepare(instanceId, propId);
            }

            @Override
            public boolean reqAccept(int instanceId, Proposal proposal) throws IOException {
                return acceptor.reqAccept(instanceId, proposal);
            }

            @Override
            public int getLastInstance() throws IOException {
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
