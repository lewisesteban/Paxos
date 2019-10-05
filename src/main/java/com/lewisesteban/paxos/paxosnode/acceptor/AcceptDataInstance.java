package com.lewisesteban.paxos.paxosnode.acceptor;

import com.lewisesteban.paxos.paxosnode.proposer.Proposal;

class AcceptDataInstance {

    private Proposal.ID lastPreparedPropId = Proposal.ID.noProposal();
    private Proposal lastAcceptedProp = null;

    AcceptDataInstance() { }

    void setLastPreparedPropId(Proposal.ID lastPreparedPropId) {
        this.lastPreparedPropId.set(lastPreparedPropId);
    }

    void setLastAcceptedProp(Proposal lastAcceptedProp) {
        this.lastAcceptedProp = lastAcceptedProp;
    }

    final Proposal.ID getLastPreparedPropId() {
        return lastPreparedPropId;
    }

    Proposal getLastAcceptedProp() {
        return lastAcceptedProp;
    }
}
