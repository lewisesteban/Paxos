package com.lewisesteban.paxos.node.proposer;

import com.lewisesteban.paxos.InstId;
import com.lewisesteban.paxos.node.MembershipGetter;
import com.lewisesteban.paxos.node.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.rpc.PaxosProposer;
import com.lewisesteban.paxos.rpc.RemotePaxosNode;

import java.io.IOException;
import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class Proposer implements PaxosProposer {

    private MembershipGetter memberList;
    private ProposalFactory propFac;

    public Proposer(MembershipGetter memberList) {
        this.memberList = memberList;
        this.propFac = new ProposalFactory(memberList.getMyNodeId());
    }

    public boolean propose(InstId instanceId, Serializable proposalData) {
        Proposal proposal = propFac.make(proposalData);
        Proposal prepared = prepare(instanceId, proposal);
        if (prepared == null) {
            return false;
        }
        boolean proposalChanged = !proposal.getData().equals(prepared.getData());
        boolean success = accept(instanceId, prepared);
        return success && !proposalChanged;
    }

    private Proposal prepare(InstId instanceId, Proposal proposal) {
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

    private boolean accept(InstId instanceId, Proposal proposal) {
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
