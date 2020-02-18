package com.lewisesteban.paxos.paxosnode.proposer;

import com.lewisesteban.paxos.paxosnode.Command;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RunningProposalManager {
    private Set<Long> instances = new TreeSet<>();
    private Proposer proposer = null;
    private Object listener = null;
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private ExecutorService executorService = Executors.newCachedThreadPool();

    public RunningProposalManager() { }

    public void setup(Proposer proposer, Object listener) {
        this.proposer = proposer;
        this.listener = listener;
    }

    /**
     * Asynchronously sends a NoOp request, ignoring its result.
     * The NoOp proposal, if any, is already started in the manager before this method returns.
     */
    public void tryProposeNoOp(long instance) {
        lock.writeLock().lock();
        try {
            try {
                startProposal(instance);
            } catch (InstanceAlreadyRunningException e) {
                return;
            }
            executorService.submit(() -> {
                try {
                    proposer.propose(Command.NoOpCommand(), instance, true);
                } catch (IOException ignored) { }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    void paxosFinished(long instance) {
        lock.writeLock().lock();
        try {
            instances.remove(instance);
        } finally {
            lock.writeLock().unlock();
        }
        //noinspection SynchronizeOnNonFinalField
        synchronized (listener) {
            listener.notifyAll();
        }
    }

    void startProposal(long instance) throws InstanceAlreadyRunningException {
        lock.writeLock().lock();
        try {
            if (instances.contains(instance)) {
                throw new InstanceAlreadyRunningException();
            }
            instances.add(instance);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean contains(long instance) {
        lock.readLock().lock();
        try {
            return instances.contains(instance);
        } finally {
            lock.readLock().unlock();
        }
    }

    class InstanceAlreadyRunningException extends IOException {
        InstanceAlreadyRunningException() {
        }
    }
}
