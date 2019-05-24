package com.lewisesteban.paxos.node.acceptor;

import com.lewisesteban.paxos.InstId;
import com.lewisesteban.paxos.node.InstanceVector;
import com.lewisesteban.paxos.node.MembershipGetter;
import com.lewisesteban.paxos.node.proposer.Proposal;
import com.lewisesteban.paxos.rpc.AcceptorRPCHandle;

public class Acceptor implements AcceptorRPCHandle {

    private InstanceVector<AcceptData> instances = new InstanceVector<>(AcceptData::new);
    private MembershipGetter memberList;

    public Acceptor(MembershipGetter memberList) {
        this.memberList = memberList;
    }

    public PrepareAnswer reqPrepare(InstId instanceNb, Proposal.ID propId) {
        AcceptData thisInstance = instances.get(instanceNb);
        if (propId.isGreaterThan(thisInstance.lastSeenPropId)) {
            thisInstance.lastSeenPropId.set(propId);
            return new PrepareAnswer(true, thisInstance.lastAcceptedProp);
        } else {
            return new PrepareAnswer(false, null);
        }
    }

    public boolean reqAccept(InstId instanceNb, Proposal proposal) {
        AcceptData thisInstance = instances.get(instanceNb);
        if (thisInstance.lastSeenPropId.isGreaterThan(proposal.getId())) {
            return false;
        } else {
            thisInstance.lastAcceptedProp = proposal;
            return true;
        }
    }

    private class AcceptData {
        Proposal.ID lastSeenPropId = Proposal.ID.noProposal();
        Proposal lastAcceptedProp = null;
    }
}
