package com.lewisesteban.paxos.node.proposer;

import com.lewisesteban.paxos.node.MembershipGetter;
import com.lewisesteban.paxos.node.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.rpc.PaxosProposer;
import com.lewisesteban.paxos.rpc.RemotePaxosNode;

import java.io.IOException;
import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Proposer implements PaxosProposer {

    private MembershipGetter memberList;
    private ProposalFactory propFac;

    public Proposer(MembershipGetter memberList) {
        this.memberList = memberList;
        this.propFac = new ProposalFactory(memberList.getMyNodeId());
    }

    public boolean propose(long instanceId, Serializable proposalData) {
        Proposal proposal = propFac.make(proposalData);
        AtomicInteger nbOk = new AtomicInteger(0);
        Queue<Proposal> alreadyAcceptedProps = new ConcurrentLinkedQueue<>();
        for (RemotePaxosNode node : memberList.getMembers()) {
            try {
                PrepareAnswer answer = node.getAcceptor().reqPrepare(instanceId, proposal.getId()); // TODO should be async
                if (answer.isPrepareOK()) {
                    nbOk.getAndIncrement();
                    if (answer.getAlreadyAccepted() != null) {
                        alreadyAcceptedProps.add(answer.getAlreadyAccepted());
                    }
                } else {
                    // Someone else is proposing: abandon proposal. May try again in another instance of Paxos.
                }
            } catch (IOException ignored) {
                // Connection with server lost.
            }
        }

        if (nbOk.get() > memberList.getNbMembers() / 2) {
            boolean propChanged = updateMyProp(alreadyAcceptedProps, proposal);
            boolean accepted = accept(instanceId, proposal);
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
            if (highestIdProp.getData().equals(myProp.getData())) {
                return false;
            } else {
                myProp.setData(highestIdProp);
                return true;
            }
        } else {
            return false;
        }
    }

    private boolean accept(long instanceId, Proposal proposal) {
        AtomicInteger nbOk = new AtomicInteger(0);
        for (RemotePaxosNode node : memberList.getMembers()) {
            try {
                if (node.getAcceptor().reqAccept(instanceId, proposal)) {
                    nbOk.incrementAndGet();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return nbOk.get() > memberList.getNbMembers() / 2;
    }
}
