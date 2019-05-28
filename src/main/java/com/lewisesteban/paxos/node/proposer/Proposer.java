package com.lewisesteban.paxos.node.proposer;

import com.lewisesteban.paxos.node.MembershipGetter;
import com.lewisesteban.paxos.node.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.rpc.PaxosProposer;
import com.lewisesteban.paxos.rpc.RemotePaxosNode;

import java.io.IOException;
import java.io.Serializable;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class Proposer implements PaxosProposer {

    private MembershipGetter memberList;
    private ProposalFactory propFac;
    private LastInstId lastInstId = new LastInstId(0);

    public Proposer(MembershipGetter memberList) {
        this.memberList = memberList;
        this.propFac = new ProposalFactory(memberList.getMyNodeId());
    }

    public Result proposeNew(Serializable proposalData) {
        return propose(proposalData, lastInstId.getAndIncrement(), true);
    }

    public Result propose(Serializable proposalData, int instanceId) {
        return propose(proposalData, instanceId, false);
    }

    private Result propose(Serializable proposalData, int instanceId, boolean newInstance) {

        Proposal originalProposal = propFac.make(proposalData);
        Proposal prepared = prepare(instanceId, originalProposal);

        if (newInstance && (prepared == null || !proposalData.equals(prepared.getData()))) {
            updateLastInstId();
            return propose(proposalData, lastInstId.getAndIncrement());
        } else if (prepared == null) {
            return new Result(false, instanceId);
        }

        boolean proposalChanged = !originalProposal.getData().equals(prepared.getData());
        boolean success = accept(instanceId, prepared);
        if (success) {
            for (RemotePaxosNode node : memberList.getMembers()) {
                node.getListener().informConsensus(instanceId, prepared.getData());
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
        final Semaphore anyThread = new Semaphore(memberList.getNbMembers());
        for (RemotePaxosNode node : memberList.getMembers()) {
            Thread thread = new Thread(() -> {
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
            try {
                anyThread.acquire();
            } catch (InterruptedException ignored) { /* should not happen */ }
            thread.start();
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
            return new Proposal(highestIdProp.getData(), originalProp.getId());
        } else {
            return originalProp;
        }
    }

    private boolean accept(int instanceId, Proposal proposal) {
        final AtomicInteger nbOk = new AtomicInteger(0);
        final Semaphore anyThread = new Semaphore(memberList.getNbMembers());
        for (RemotePaxosNode node : memberList.getMembers()) {
            Thread thread = new Thread(() -> {
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
            try {
                anyThread.acquire();
            } catch (InterruptedException ignored) { /* should not happen */ }
            thread.start();
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
}
