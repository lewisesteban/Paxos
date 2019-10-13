package com.lewisesteban.paxos.paxosnode.proposer;

import com.lewisesteban.paxos.paxosnode.Command;

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

    Proposal make(Command command) {
        return new Proposal(command, new Proposal.ID(myNodeId, proposalNumber.incrementAndGet()));
    }
}
