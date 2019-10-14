package com.lewisesteban.paxos.paxosnode.acceptor;

import com.lewisesteban.paxos.Logger;
import com.lewisesteban.paxos.storage.StorageUnit;
import com.lewisesteban.paxos.paxosnode.InstanceVector;
import com.lewisesteban.paxos.paxosnode.MembershipGetter;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;

public class Acceptor implements AcceptorRPCHandle {

    private InstanceVector<AcceptDataInstance> instances = new InstanceVector<>(AcceptDataInstance::new);
    private MembershipGetter memberList;
    private StorageUnit storage;

    public Acceptor(MembershipGetter memberList, StorageUnit storage) {
        this.memberList = memberList;
        this.storage = storage;
    }

    public PrepareAnswer reqPrepare(long instanceNb, Proposal.ID propId) {
        AcceptDataInstance thisInstance = instances.get(instanceNb);
        synchronized (thisInstance) {
            if (propId.isGreaterThan(thisInstance.getLastPreparedPropId())) {
                thisInstance.setLastPreparedPropId(propId);
                Logger.println("--o inst " + instanceNb + " srv " + memberList.getMyNodeId() + " prepare OK " + propId);
                return new PrepareAnswer(true, thisInstance.getLastAcceptedProp());
            } else {
                Logger.println("--o inst " + instanceNb + " srv " + memberList.getMyNodeId() + " prepare NOK " + propId);
                return new PrepareAnswer(false, null);
            }
        }
    }

    public boolean reqAccept(long instanceNb, Proposal proposal) {
        AcceptDataInstance thisInstance = instances.get(instanceNb);
        synchronized (thisInstance) {
            if (thisInstance.getLastPreparedPropId().isGreaterThan(proposal.getId())) {
                Logger.println("--o inst " + instanceNb + " srv " + memberList.getMyNodeId() + " REFUSE " + proposal.getCommand() + " last seen prop id = " + thisInstance.getLastPreparedPropId() + " this prop id = " + proposal.getId());
                return false;
            } else {
                thisInstance.setLastAcceptedProp(proposal);
                Logger.println("--- inst " + instanceNb + " srv " + memberList.getMyNodeId() + " accept " + proposal.getCommand());
                return true;
            }
        }
    }

    @Override
    public long getLastInstance() {
        return instances.getHighestInstance();
    }
}
