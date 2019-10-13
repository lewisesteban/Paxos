package com.lewisesteban.paxos.paxosnode.proposer;

import com.lewisesteban.paxos.Logger;
import com.lewisesteban.paxos.paxosnode.MembershipGetter;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.listener.Listener;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.storage.StorageUnit;

import java.io.IOException;
import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

// TODO give cmd unique ID used when checking if a command has changed (after prepare)
public class Proposer implements PaxosProposer {

    private MembershipGetter memberList;
    private ProposalFactory propFac;
    private ExecutorService executor = Executors.newCachedThreadPool();
    private Listener listener;
    private StorageUnit storage;

    public Proposer(MembershipGetter memberList, Listener listener, StorageUnit storage) {
        this.memberList = memberList;
        this.propFac = new ProposalFactory(memberList.getMyNodeId());
        this.listener = listener;
        this.storage = storage;
    }

    public long getNewInstanceId() {
        return listener.getLastInstanceId() + 1;
    }

    public Result propose(Serializable command, long instanceId) {

        Logger.println("#instance " + instanceId + " proposal: " + command);

        Listener.ExecutedCommand thisInstanceExecutedCmd = listener.tryGetExecutedCommand(instanceId);
        if (thisInstanceExecutedCmd != null) {
            boolean sameCmd = (thisInstanceExecutedCmd.getCommand().equals(command));
            return new Result(sameCmd ? Result.CONSENSUS_ON_THIS_CMD : Result.CONSENSUS_ON_ANOTHER_CMD,
                    instanceId, thisInstanceExecutedCmd.getResult());
        }

        Proposal originalProposal = propFac.make(command);
        //System.out.println("proposer " + memberList.getMyNodeId() + " inst=" + instanceId + " starting cmd=" + command.toString() + " proposalId=" + originalProposal.getId());
        Proposal prepared = prepare(instanceId, originalProposal);
        if (prepared == null) {
            return new Result(Result.CONSENSUS_FAILED, instanceId);
        }

        boolean proposalChanged = !originalProposal.getCommand().equals(prepared.getCommand());
        //System.out.println("proposer " + memberList.getMyNodeId() + " inst=" + instanceId + " prepared changed = " + proposalChanged + " cmd=" + command.toString() + " proposalId=" + prepared.getId());
        boolean success = accept(instanceId, prepared);
        //System.out.println("proposer " + memberList.getMyNodeId() + " inst=" + instanceId + " accept success = " + success);
        if (success) {
            scatter(instanceId, prepared);
            //System.out.println("proposer " + memberList.getMyNodeId() + " inst=" + instanceId + " scattered");
            if (proposalChanged) {
                Logger.println(">>> inst " + instanceId + " proposal changed from " + originalProposal.getCommand().toString() + " to " + prepared.getCommand().toString());
            }
            java.io.Serializable returnData = null;
            if (!proposalChanged) {
                returnData = listener.getReturnOf(instanceId, prepared.getCommand());
            }
            //System.out.println("proposer " + memberList.getMyNodeId() + " inst=" + instanceId + " cmd=" + prepared.getCommand() + " final result : " + (proposalChanged ? "changed" : "success"));
            return new Result(proposalChanged ? Result.CONSENSUS_ON_ANOTHER_CMD : Result.CONSENSUS_ON_THIS_CMD,
                    instanceId, returnData);
        } else {
            //System.out.println("proposer " + memberList.getMyNodeId() + " inst=" + instanceId + " cmd=" + prepared.getCommand() + " final result : failed");
            return new Result(Result.CONSENSUS_FAILED, instanceId);
        }
    }

    private Proposal prepare(long instanceId, Proposal proposal) {
        final AtomicInteger nbOk = new AtomicInteger(0);
        final Queue<Proposal> alreadyAcceptedProps = new ConcurrentLinkedQueue<>();
        final Semaphore anyThread = new Semaphore(0);
        for (RemotePaxosNode node : memberList.getMembers()) {
            executor.submit(() -> {
                try {
                    PrepareAnswer answer = node.getAcceptor().reqPrepare(instanceId, proposal.getId());
                    if (answer.isPrepareOK()) {
                        nbOk.getAndIncrement();
                        if (answer.getAlreadyAccepted() != null) {
                            alreadyAcceptedProps.add(answer.getAlreadyAccepted());
                        }
                    } else {
                        // Someone else is proposing: abandon proposal.
                        anyThread.release(memberList.getNbMembers());
                    }
                } catch (IOException ignored) {
                    // connection with server lost
                } finally {
                    anyThread.release();
                }
            });
        }

        int runningThreads = memberList.getNbMembers();
        while (nbOk.get() <= memberList.getNbMembers() / 2 && runningThreads > 0) {
            try {
                anyThread.acquire();
                runningThreads--;
            } catch (InterruptedException ignored) { }
        }

        if (nbOk.get() > memberList.getNbMembers() / 2) {
            return getNewProp(alreadyAcceptedProps, proposal);
        }
        return null;
    }

    private Proposal getNewProp(final Queue<Proposal> alreadyAcceptedProps, Proposal originalProp) {
        if (!alreadyAcceptedProps.isEmpty()) {
            Proposal highestIdProp = null;
            for (Proposal prop : alreadyAcceptedProps) {
                if (highestIdProp == null || prop.getId().isGreaterThan(highestIdProp.getId())) {
                    highestIdProp = prop;
                }
            }
            return new Proposal(highestIdProp.getCommand(), originalProp.getId());
        } else {
            return originalProp;
        }
    }

    private boolean accept(long instanceId, Proposal proposal) {
        final AtomicInteger nbOk = new AtomicInteger(0);
        final Semaphore anyThread = new Semaphore(0);
        for (RemotePaxosNode node : memberList.getMembers()) {
            executor.submit(() -> {
                try {
                    if (node.getAcceptor().reqAccept(instanceId, proposal)) {
                        nbOk.incrementAndGet();
                    }
                } catch (IOException ignored) {
                    // connection with server lost
                } finally {
                    anyThread.release();
                }
            });
        }

        int runningThreads = memberList.getNbMembers();
        while (nbOk.get() <= memberList.getNbMembers() / 2 && runningThreads > 0) {
            try {
                anyThread.acquire();
                runningThreads--;
            } catch (InterruptedException ignored) { }
        }
        return nbOk.get() > memberList.getNbMembers() / 2;
    }

    private void scatter(long instanceId, Proposal prepared) {
        Future[] threads = new Future[memberList.getNbMembers()];
        int threadIt = 0;
        for (RemotePaxosNode node : memberList.getMembers()) {
            threads[threadIt] = executor.submit(() -> {
                try {
                    node.getListener().execute(instanceId, prepared.getCommand());
                } catch (IOException e) {
                    // TODO what to do about failures?
                }
            });
            threadIt++;
        }
        for (Future thread : threads) {
            try {
                thread.get(); // should we wait for everyone?
            } catch (ExecutionException | InterruptedException ignored) { }
        }
    }
}
