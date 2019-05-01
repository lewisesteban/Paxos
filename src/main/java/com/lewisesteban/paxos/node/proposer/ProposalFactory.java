package com.lewisesteban.paxos.node.proposer;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class ProposalFactory {

    private AtomicLong proposalNumber = new AtomicLong(0);
    private int myNodeId;

    ProposalFactory(int myNodeId) {
        this.myNodeId = myNodeId;
    }

    void updateProposalNumber(long newVal) {
        proposalNumber.set(newVal);
    }

    Proposal make(Serializable data) {
        return new Proposal(data, new Proposal.ID(myNodeId, proposalNumber.incrementAndGet()));
    }
}
