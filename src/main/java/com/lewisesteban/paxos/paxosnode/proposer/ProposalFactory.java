package com.lewisesteban.paxos.paxosnode.proposer;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;

import java.util.concurrent.atomic.AtomicInteger;

class ProposalFactory {

    private AtomicInteger proposalNumber = new AtomicInteger(0);
    private int myNodeId;
    private StorageUnit storage;

    private static final String STORAGE_KEY_PROPOSAL_NB = "proposalNb";

    ProposalFactory(int myNodeId, StorageUnit storage) throws StorageException {
        this.myNodeId = myNodeId;
        this.storage = storage;
        String propNbStr = storage.read(STORAGE_KEY_PROPOSAL_NB);
        if (propNbStr != null) {
            proposalNumber.set(Integer.parseInt(propNbStr));
        }
    }

    Proposal make(Command command) throws StorageException {
        int commandsProposalNb = proposalNumber.getAndIncrement();
        storage.put(STORAGE_KEY_PROPOSAL_NB, String.valueOf(proposalNumber.get()));
        storage.flush();
        return new Proposal(command, new Proposal.ID(myNodeId, commandsProposalNb));
    }
}
