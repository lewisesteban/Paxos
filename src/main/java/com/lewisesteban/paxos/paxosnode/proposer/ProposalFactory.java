package com.lewisesteban.paxos.paxosnode.proposer;

import com.lewisesteban.paxos.paxosnode.Command;

import java.util.concurrent.atomic.AtomicLong;

class ProposalFactory {

    private AtomicLong proposalNumber = new AtomicLong(0);
    private int myNodeId;

    ProposalFactory(int myNodeId) {
        this.myNodeId = myNodeId;
    }

    Proposal make(Command command) {
        long commandsProposalNb = proposalNumber.getAndIncrement();
        return new Proposal(command, new Proposal.ID(myNodeId, commandsProposalNb));
    }

    void updateProposalNumber(long newVal) {
        long oldVal = proposalNumber.get();
        while (newVal > oldVal) {
            proposalNumber.compareAndSet(oldVal, newVal);
            oldVal = proposalNumber.get();
        }
    }
}
