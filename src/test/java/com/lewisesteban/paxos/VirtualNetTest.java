package com.lewisesteban.paxos;

import com.lewisesteban.paxos.node.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.node.proposer.Proposal;
import com.lewisesteban.paxos.rpc.AcceptorRPCHandle;
import com.lewisesteban.paxos.rpc.RemotePaxosNode;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import junit.framework.TestCase;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.lewisesteban.paxos.virtualnet.server.PaxosServer.SRV_FAILURE_MSG;

public class VirtualNetTest extends TestCase {

    private boolean slowPropose = false;
    private int slowAcceptorId = 0;

    public void testKillProposingServer() throws InterruptedException {

        final int NB_CLIENTS = 5;
        final AtomicInteger nbSuccesses = new AtomicInteger(0);
        final AtomicInteger nbCorrectExceptions = new AtomicInteger(0);

        Network network = new Network();
        List<PaxosNetworkNode> nodes = NetworkFactory.initSimpleNetwork(2, network, SlowPaxosNode::new);
        final PaxosNetworkNode server = nodes.get(0);

        slowPropose = true;
        List<Thread> clients = new ArrayList<>();
        for (int i = 0; i < NB_CLIENTS; ++i) {
            final String proposalData = String.valueOf(i);
            clients.add(new Thread(() -> {
                try {
                    server.getPaxosSrv().propose(proposalData);
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
        Thread.sleep(100); // wait for the propose/accept tasks to start
        System.out.println("Enough waiting. Kill the servers.");

        network.kill(0);
        for (Thread client : clients) {
            try {
                client.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        assert nbSuccesses.get() == 0;
        assert nbCorrectExceptions.get() == NB_CLIENTS;
    }

    public void testKillAcceptingServer() throws InterruptedException, ExecutionException {

        Network network = new Network();
        List<PaxosNetworkNode> nodes = NetworkFactory.initSimpleNetwork(2, network, SlowPaxosNode::new);
        slowAcceptorId = 1;
        final PaxosNetworkNode server = nodes.get(0);

        slowPropose = false;
        FutureTask<Boolean> client = new FutureTask<>(() -> server.getPaxosSrv().propose("Proposal"));
        new Thread(client).start();

        System.out.println("Task started. Let's wait a bit...");
        Thread.sleep(100); // wait for the propose/accept tasks to start
        System.out.println("Enough waiting. Kill the servers.");

        network.kill(slowAcceptorId);
        assert !client.get();
    }

    private class SlowPaxosNode extends PaxosNode {

        private AcceptorRPCHandle acceptor;

        SlowPaxosNode(int id, List<RemotePaxosNode> remotePaxosNodeList) {
            super(id, remotePaxosNodeList);
            acceptor = new SlowPaxosAcceptor(getAcceptor());
        }

        @Override
        public boolean propose(Serializable data) throws IOException {
            if (slowPropose) {
                doHeavyWork();
            }
            return super.propose(data);
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
            public PrepareAnswer reqPrepare(Proposal.ID propId) throws IOException {
                if (getId() == slowAcceptorId) {
                    doHeavyWork();
                }
                return acceptor.reqPrepare(propId);
            }

            @Override
            public boolean reqAccept(Proposal proposal) throws IOException {
                return acceptor.reqAccept(proposal);
            }
        }

        void doHeavyWork() {
            @SuppressWarnings("MismatchedQueryAndUpdateOfStringBuilder")
            StringBuilder str = new StringBuilder("");
            for (int i = 0; i < 100000000; ++i) {
                if (i % 1000 == 0) {
                    str = new StringBuilder("");
                }
                str.append(String.valueOf(i));
            }
        }
    }
}
