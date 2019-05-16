package com.lewisesteban.paxos.node.proposer;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class ProposalFactory {

    private AtomicInteger proposalNumber = new AtomicInteger(0);
    private int myNodeId;

    ProposalFactory(int myNodeId) {
        this.myNodeId = myNodeId;
    }

    void updateProposalNumber(int newVal) {
        proposalNumber.set(newVal);
    }

    Proposal make(Serializable data) {
        return new Proposal(data, new Proposal.ID(myNodeId, proposalNumber.incrementAndGet()));
    }
}
