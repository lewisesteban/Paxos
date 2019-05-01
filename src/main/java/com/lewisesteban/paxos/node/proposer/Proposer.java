package com.lewisesteban.paxos.node.proposer;

import com.lewisesteban.paxos.node.MembershipGetter;
import com.lewisesteban.paxos.node.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.rpc.NodeRPCHandle;

import java.io.IOException;
import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Proposer {

    private MembershipGetter memberList;
    private ProposalFactory propFac;

    public Proposer(MembershipGetter memberList) {
        this.memberList = memberList;
        this.propFac = new ProposalFactory(memberList.getMyNodeId());
    }

    public boolean propose(Serializable proposalData) {
        Proposal proposal = propFac.make(proposalData);
        AtomicInteger nbOk = new AtomicInteger(0);
        Queue<Proposal> alreadyAcceptedProps = new ConcurrentLinkedQueue<Proposal>();
        for (NodeRPCHandle node : memberList.getMembers()) {
            try {
                PrepareAnswer answer = node.getAcceptor().reqPrepare(proposal.getId()); // TODO should be async
                if (answer.isPrepareOK()) {
                    nbOk.getAndIncrement();
                    if (answer.getAlreadyAccepted() != null) {
                        alreadyAcceptedProps.add(answer.getAlreadyAccepted());
                    }
                } else {
                    // Someone else is proposing: abandon proposal. May try again in another instance of Paxos.
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (nbOk.get() > memberList.getNbMembers() / 2) {
            boolean propChanged = updateMyProp(alreadyAcceptedProps, proposal);
            boolean accepted = accept(proposal);
            return (accepted && !propChanged);
        }

        return false;
    }

    private boolean updateMyProp(Queue<Proposal> alreadyAcceptedProps, Proposal myProp) {
        if (!alreadyAcceptedProps.isEmpty()) {
            Proposal highestIdProp = null;
            for (Proposal prop : alreadyAcceptedProps) {
                if (highestIdProp == null || prop.getId().isGreaterThan(highestIdProp.getId())) {
                    highestIdProp = prop;
                }
            }
            myProp.setData(highestIdProp);
            return true;
        } else {
            return false;
        }
    }

    private boolean accept(Proposal proposal) {
        AtomicInteger nbOk = new AtomicInteger(0);
        for (NodeRPCHandle node : memberList.getMembers()) {
            try {
                if (node.getAcceptor().reqAccept(proposal)) {
                    nbOk.incrementAndGet();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return nbOk.get() > memberList.getNbMembers() / 2;
    }
}
