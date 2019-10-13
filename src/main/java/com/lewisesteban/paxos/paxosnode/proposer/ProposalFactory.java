package com.lewisesteban.paxos.paxosnode.proposer;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

class ProposalFactory {

    private AtomicInteger proposalNumber = new AtomicInteger(0); // TODO store to disk
    private int myNodeId;

    ProposalFactory(int myNodeId) {
        this.myNodeId = myNodeId;
    }

    void updateProposalNumber(int newVal) {
        proposalNumber.set(newVal);
    }

    Proposal make(Serializable command) {
        return new Proposal(command, new Proposal.ID(myNodeId, proposalNumber.incrementAndGet()));
    }
}
