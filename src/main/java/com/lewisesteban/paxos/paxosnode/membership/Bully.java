package com.lewisesteban.paxos.paxosnode.membership;

import com.lewisesteban.paxos.Logger;
import com.lewisesteban.paxos.paxosnode.ClusterHandle;
import com.lewisesteban.paxos.rpc.paxos.MembershipRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Bully {
    public static int WAIT_FOR_VICTORY_TIMEOUT = 1000;
    public static int WAIT_AFTER_FAILURE = 500;

    private ClusterHandle cluster;
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private AtomicBoolean electionIsOngoing = new AtomicBoolean(false);
    private AtomicInteger electionInstance = new AtomicInteger(0);
    private AtomicInteger lastElectionVoted = new AtomicInteger(-1);

    Bully(ClusterHandle cluster) {
        this.cluster = cluster;
    }

    /**
     * Asynchronous, returns immediately.
     * Starts a new round of election if none is already ongoing.
     * Sets the electionInstance and electionIsOngoing before returning.
     */
    void startElection() {
        if (electionIsOngoing.compareAndSet(false, true)) {
            electionInstance.incrementAndGet();
            executorService.submit(() -> {
                if (cluster.getMyNodeId() == cluster.getNbMembers() - 1) {
                    declareVictory();
                } else {
                    sendElectionMessages();
                }
            });
        }
    }

    synchronized MembershipRPCHandle.BullyVictoryResponse receiveVictoryMessage(int senderId, int msgInstanceNb) {
        if (msgInstanceNb >= electionInstance.get() && lastElectionVoted.get() < msgInstanceNb) {
            lastElectionVoted.set(msgInstanceNb);
            electionInstance.set(msgInstanceNb);
            cluster.setLeaderNodeId(senderId);
            electionIsOngoing.set(false);
            this.notifyAll();
            return new MembershipRPCHandle.BullyVictoryResponse(true, electionInstance.get());
        }
        return new MembershipRPCHandle.BullyVictoryResponse(false, electionInstance.get());
    }

    synchronized int receiveElectionMessage(int instanceNb) {
        if (instanceNb > electionInstance.get()) {
            startElection();
        }
        return electionInstance.get();
    }

    private void sendElectionMessages() {
        for (RemotePaxosNode node : cluster.getMembers()) {
            if (electionIsOngoing.get() && node.getId() > cluster.getMyNodeId()) {
                try {
                    int remoteInstance = node.getMembership().bullyElection(electionInstance.get());
                    if (remoteInstance == electionInstance.get()) {
                        waitForVictoryMessage();
                        return;
                    } else {
                        // my election nb is late -- let Membership start a new election if necessary
                        electionInstance.set(remoteInstance);
                        electionIsOngoing.set(false);
                        return;
                    }
                } catch (IOException ignored) { }
            }
        }
        // no node with higher ID can be the leader
        if (electionIsOngoing.get())
            declareVictory();
    }

    private synchronized void waitForVictoryMessage() {
        int inst = electionInstance.get();
        long startTime = System.currentTimeMillis();
        while (electionInstance.get() == inst && electionIsOngoing.get() && cluster.getLeaderNodeId() == null) {
            long timeLeft = WAIT_FOR_VICTORY_TIMEOUT - (System.currentTimeMillis() - startTime);
            if (timeLeft > 0) {
                try {
                    wait(timeLeft);
                } catch (InterruptedException ignored) { }
            } else {
                // timed out - a new election will start automatically if necessary
                electionIsOngoing.set(false);
                return;
            }
        }
    }

    private void declareVictory() {
        cluster.setLeaderNodeId(cluster.getMyNodeId());
        electionIsOngoing.set(false);

        // vote for self
        AtomicInteger successfulNotifications = new AtomicInteger(1);
        lastElectionVoted.set(electionInstance.get());

        AtomicInteger runningThreads = new AtomicInteger(cluster.getNbMembers() - 1);
        for (RemotePaxosNode node : cluster.getMembers()) {
            if (node.getId() != cluster.getMyNodeId()) {
                executorService.submit(() -> {
                    try {
                        MembershipRPCHandle.BullyVictoryResponse response = node.getMembership().bullyVictory(cluster.getMyNodeId(), electionInstance.get());
                        if (response.isVictoryAccepted())
                            successfulNotifications.incrementAndGet();
                        else if (response.getInstanceNb() > electionInstance.get())
                            electionInstance.set(response.getInstanceNb());
                    } catch (IOException ignored) {
                    } finally {
                        synchronized (this) {
                            runningThreads.decrementAndGet();
                            notifyAll();
                        }
                    }
                });
            }
        }
        synchronized (this) {
            while (runningThreads.get() > 0 && successfulNotifications.get() <= cluster.getNbMembers() / 2) {
                try {
                    wait();
                } catch (InterruptedException ignored) {
                }
            }
        }

        if (successfulNotifications.get() > cluster.getNbMembers() / 2) {
            Logger.println("NEW LEADER: " + cluster.getMyNodeId());
            return;
        }

        try {
            Thread.sleep(WAIT_AFTER_FAILURE);
        } catch (InterruptedException ignored) {
        }
        if (!(cluster.getLeaderNodeId() > cluster.getMyNodeId())) // make sure I haven't already voted again
            cluster.setLeaderNodeId(null); // a new election will start automatically if necessary
    }

    // bully algorithm:
    // if I have the highest ID, I send a declareVictory message to all other processes, telling them I'm the leader
    // otherwise, I send an election message to processes with a higher ID
    // then I wait for an answer. If I timeout, I restart a new round of election.
    // If I receive no positive answer, I become the leader and send a declareVictory message to all other processes
    // If I receive an election message from a process with a lower ID, I answer and then start the election (From line 1)

    // partitioning solution:
    // If I'm sending declareVictory messages, but I don't get a majority of them to succeed and less than a majority of
    // servers (including myself) know I'm the leader, then I wait a little bit then restart the election (if it hasn't
    // restarted already)

    // catching-up solution:
    // each election round (instance) has a number
    // when receiving an election message, don't start a new election unless the received number is higher than currant
    // when receiving a victory message, if its election number is less than currant, then refuse it (return false)
    // otherwise, if already voted for this round, return false; otherwise, return true and update last voted round
    // when sending victory messages, only those which return true count
    // whenever an election number higher than currant is received, update currant
}
