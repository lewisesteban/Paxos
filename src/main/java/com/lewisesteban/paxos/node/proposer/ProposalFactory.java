package com.lewisesteban.paxos.node.proposer;

import com.lewisesteban.paxos.Command;

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

    Proposal make(Command command) {
        return new Proposal(command, new Proposal.ID(myNodeId, proposalNumber.incrementAndGet()));
    }
}
