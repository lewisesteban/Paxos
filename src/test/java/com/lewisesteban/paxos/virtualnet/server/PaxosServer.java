package com.lewisesteban.paxos.virtualnet.server;

import com.lewisesteban.paxos.PaxosNode;
import com.lewisesteban.paxos.rpc.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * A virtual paxos server running an instance of PaxosNode.
 * When called from this class, code from the PaxosNode class is run on separate threads.
 * A server failure can be simulated by calling kill()
 */
public class PaxosServer implements PaxosProposer, RemotePaxosNode {

    private PaxosSrvAcceptor acceptor;
    private PaxosSrvListener listener;
    private PaxosSrvMembership membership;

    private PaxosNode paxos;

    private ExecutorService threadPool = Executors.newFixedThreadPool(4, new DaemonThreadFactory());

    public PaxosServer(PaxosNode paxos) {
        this.paxos = paxos;
        acceptor = new PaxosSrvAcceptor(paxos.getAcceptor(), threadPool);
        listener = new PaxosSrvListener(paxos.getListener(), threadPool);
        membership = new PaxosSrvMembership(paxos.getMembership(), threadPool);
    }

    public void start() {
        threadPool = Executors.newFixedThreadPool(4);
        paxos.start();
    }

    public void stop() {
        paxos.stop();
        threadPool.shutdown();
    }

    public void kill() {
        threadPool.shutdownNow();
        paxos.stopNow();
    }

    @Override
    public boolean propose(final Serializable proposalData) throws IOException {
        System.out.println("PaxosServer propose");
        try {
            return threadPool.submit(() -> paxos.propose(proposalData)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public int getId() {
        return paxos.getId();
    }

    @Override
    public AcceptorRPCHandle getAcceptor() {
        return acceptor;
    }

    @Override
    public ListenerRPCHandle getListener() {
        return listener;
    }

    @Override
    public MembershipRPCHandle getMembership() {
        return membership;
    }

    static class DaemonThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("Daemon Thread");
            t.setDaemon(true);
            return t;
        }
    }
}
