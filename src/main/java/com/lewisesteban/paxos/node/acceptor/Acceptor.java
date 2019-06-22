package com.lewisesteban.paxos.node.acceptor;

import com.lewisesteban.paxos.Logger;
import com.lewisesteban.paxos.node.InstanceVector;
import com.lewisesteban.paxos.node.MembershipGetter;
import com.lewisesteban.paxos.node.proposer.Proposal;
import com.lewisesteban.paxos.rpc.AcceptorRPCHandle;

public class Acceptor implements AcceptorRPCHandle {

    private InstanceVector<AcceptDataInstance> instances = new InstanceVector<>(AcceptDataInstance::new);
    private MembershipGetter memberList;

    public Acceptor(MembershipGetter memberList) {
        this.memberList = memberList;
    }

    public PrepareAnswer reqPrepare(int instanceNb, Proposal.ID propId) {
        AcceptDataInstance thisInstance = instances.get(instanceNb);
        synchronized (thisInstance) {
            if (propId.isGreaterThan(thisInstance.getLastPreparedPropId())) {
                thisInstance.setLastPreparedPropId(propId);
                return new PrepareAnswer(true, thisInstance.getLastAcceptedProp());
            } else {
                return new PrepareAnswer(false, null);
            }
        }
    }

    public boolean reqAccept(int instanceNb, Proposal proposal) {
        AcceptDataInstance thisInstance = instances.get(instanceNb);
        synchronized (thisInstance) {
            if (thisInstance.getLastPreparedPropId().isGreaterThan(proposal.getId())) {
                Logger.println("--o inst " + instanceNb + " srv " + memberList.getMyNodeId() + " REFUSE " + proposal.getData() + " last seen prop id = " + thisInstance.getLastPreparedPropId() + " this prop id = " + proposal.getId());
                return false;
            } else {
                thisInstance.setLastAcceptedProp(proposal);
                Logger.println("--- inst " + instanceNb + " srv " + memberList.getMyNodeId() + " accept " + proposal.getData());
                return true;
            }
        }
    }

    @Override
    public int getLastInstance() {
        return instances.getHighestInstance();
    }
}
