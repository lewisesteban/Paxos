package com.lewisesteban.paxos.virtualnet.server;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.PaxosNode;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.*;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A virtual paxos server running an instance of PaxosNode.
 * When called from this class, code from the PaxosNode class is run on separate threads.
 * A server failure can be simulated by calling kill()
 */
public class PaxosServer implements PaxosProposer, RemotePaxosNode {

    public static final String SRV_FAILURE_MSG = "Server failed";

    private PaxosSrvAcceptor acceptor;
    private PaxosSrvListener listener;
    private PaxosSrvMembership membership;

    private PaxosNode paxos;

    private final ThreadManager threadManager = new ThreadManager();

    public PaxosServer(PaxosNode paxos) {
        this.paxos = paxos;
        acceptor = new PaxosSrvAcceptor(paxos.getAcceptor(), threadManager);
        listener = new PaxosSrvListener(paxos.getListener(), threadManager);
        membership = new PaxosSrvMembership(paxos.getMembership(), threadManager);
    }

    public void start() {
        threadManager.start();
        paxos.start();
    }

    public void stop() {
        paxos.stop();
        threadManager.stop();
    }

    public void kill() {
        threadManager.shutDownNow();
        paxos.stopNow();
    }

    @Override
    public Result propose(final Command command, int instanceId) throws IOException {
        return threadManager.pleaseDo(() -> paxos.propose(command, instanceId));
    }

    @Override
    public Result proposeNew(final Command command) throws IOException {
        return threadManager.pleaseDo(() -> paxos.proposeNew(command));
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

    class ThreadManager {

        private ExecutorService executor = Executors.newCachedThreadPool();
        private ConcurrentSkipListSet<FutureWithId> waitingTasks = new ConcurrentSkipListSet<>();
        private AtomicInteger lastGivenId = new AtomicInteger(0);
        private boolean isRunning = true;

        <T> T pleaseDo(Callable<T> task) throws IOException {
            Future<T> future = executor.submit(task);
            // note: if server shuts down right here, this task will not be interrupted
            FutureWithId storedTask = new FutureWithId(future, lastGivenId.getAndIncrement());
            try {
                synchronized (this) {
                    if (!isRunning)
                        throw new RejectedExecutionException("Shutdown");
                    waitingTasks.add(storedTask);
                }
                return future.get();
            } catch (InterruptedException | RejectedExecutionException | CancellationException e) {
                throw new IOException(SRV_FAILURE_MSG);
            } catch (ExecutionException e) {
                throw new IOException(e);
            } finally {
                waitingTasks.remove(storedTask);
            }
        }

        synchronized void start() {
            waitingTasks = new ConcurrentSkipListSet<>();
            lastGivenId = new AtomicInteger(0);
            executor  = Executors.newCachedThreadPool();
            isRunning = true;
        }

        synchronized void stop() {
            isRunning = false;
            for (FutureWithId task : waitingTasks) {
                try {
                    task.getFuture().get();
                } catch (InterruptedException | ExecutionException ignored) { }
            }
            executor.shutdown();
        }

        synchronized void shutDownNow() {
            isRunning = false;
            for (FutureWithId task : waitingTasks) {
                task.getFuture().cancel(true);
            }
            executor.shutdownNow();
        }

        private class FutureWithId implements Comparable<FutureWithId> {

            private Future futureTask;
            private int id;

            FutureWithId(Future futureTask, int id) {
                this.futureTask = futureTask;
                this.id = id;
            }

            Future getFuture() {
                return futureTask;
            }

            @Override
            public int compareTo(FutureWithId other) {
                if (id == other.id) {
                    return 0;
                }
                return id > other.id ? 1 : -1;
            }
        }
    }

}
