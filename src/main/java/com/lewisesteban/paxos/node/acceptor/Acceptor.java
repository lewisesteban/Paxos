package com.lewisesteban.paxos.node.acceptor;

import com.lewisesteban.paxos.node.MembershipGetter;
import com.lewisesteban.paxos.node.proposer.Proposal;
import com.lewisesteban.paxos.rpc.AcceptorRPCHandle;

import java.io.IOException;

public class Acceptor implements AcceptorRPCHandle {

    private MembershipGetter memberList;

    private Proposal.ID lastSeenPropId = Proposal.ID.noProposal();
    private Proposal lastAcceptedProp = null;

    public Acceptor(MembershipGetter memberList) {
        this.memberList = memberList;
    }

    public PrepareAnswer reqPrepare(Proposal.ID propId) throws IOException {
        if (propId.isGreaterThan(lastSeenPropId)) {
            lastSeenPropId.set(propId);
            return new PrepareAnswer(true, lastAcceptedProp);
        } else {
            return new PrepareAnswer(false, null);
        }
    }

    public boolean reqAccept(Proposal proposal) {
        if (lastSeenPropId.isGreaterThan(proposal.getId())) {
            return false;
        } else {
            lastAcceptedProp = proposal;
            return true;
        }
    }
}
