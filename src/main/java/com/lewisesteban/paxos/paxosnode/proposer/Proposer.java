package com.lewisesteban.paxos.paxosnode.proposer;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.Logger;
import com.lewisesteban.paxos.paxosnode.MembershipGetter;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;

import java.io.IOException;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Proposer implements PaxosProposer {

    private MembershipGetter memberList;
    private ProposalFactory propFac;
    private LastInstId lastInstId = new LastInstId(0);
    private ExecutorService executor = Executors.newCachedThreadPool();

    public Proposer(MembershipGetter memberList) {
        this.memberList = memberList;
        this.propFac = new ProposalFactory(memberList.getMyNodeId());
    }

    public Result proposeNew(Command command) {
        return propose(command, lastInstId.getAndIncrement(), true);
    }

    public Result propose(Command command, int instanceId) {
        return propose(command, instanceId, false);
    }

    private Result propose(Command command, int instanceId, boolean newInstance) {

        Logger.println("#instance " + instanceId + " proposal: " + command);

        Proposal originalProposal = propFac.make(command);
        Proposal prepared = prepare(instanceId, originalProposal);

        if (newInstance && (prepared == null || !command.equals(prepared.getCommand()))) {
            updateLastInstId();
            return propose(command, lastInstId.getAndIncrement());
        } else if (prepared == null) {
            return new Result(false, instanceId);
        }

        boolean proposalChanged = !originalProposal.getCommand().equals(prepared.getCommand());
        boolean success = accept(instanceId, prepared);
        if (success) {
            scatter(instanceId, prepared);
            if (proposalChanged) {
                Logger.println(">>> inst " + instanceId + " proposal changed from " + originalProposal.getCommand().toString() + " to " + prepared.getCommand().toString());
            }
            return new Result(!proposalChanged, instanceId);
        } else {
            return new Result(false, instanceId);
        }
    }

    private void updateLastInstId() {
        Random randGen = new Random();
        int randInt = randGen.nextInt(memberList.getNbMembers());
        while (randInt == memberList.getMyNodeId()) {
            randInt = randGen.nextInt(memberList.getNbMembers());
        }
        RemotePaxosNode randNode = memberList.getMembers().get(randInt);
        try {
            int remoteLastInstId = randNode.getAcceptor().getLastInstance();
            lastInstId.increaseTo(remoteLastInstId);
        } catch (IOException ignored) { }
    }

    private Proposal prepare(int instanceId, Proposal proposal) {
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

    private boolean accept(int instanceId, Proposal proposal) {
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

    private void scatter(int instanceId, Proposal prepared) {
        Future[] threads = new Future[memberList.getNbMembers()];
        int threadIt = 0;
        for (RemotePaxosNode node : memberList.getMembers()) {
            threads[threadIt] = executor.submit(() -> {
                try {
                    node.getListener().informConsensus(instanceId, prepared.getCommand());
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
