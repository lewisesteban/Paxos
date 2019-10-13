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
                //System.out.println("acceptor " + memberList.getMyNodeId() + " inst=" + instanceNb + " prepare OK propId=" + propId + " cmd=" + (thisInstance.getLastAcceptedProp() == null ? "none" : thisInstance.getLastAcceptedProp().getCommand()));
                return new PrepareAnswer(true, thisInstance.getLastAcceptedProp());
            } else {
                //System.out.println("acceptor " + memberList.getMyNodeId() + " inst=" + instanceNb + " prepare NOK propId=" + propId);
                return new PrepareAnswer(false, null);
            }
        }
    }

    public boolean reqAccept(long instanceNb, Proposal proposal) {
        AcceptDataInstance thisInstance = instances.get(instanceNb);
        synchronized (thisInstance) {
            if (thisInstance.getLastPreparedPropId().isGreaterThan(proposal.getId())) {
                Logger.println("--o inst " + instanceNb + " srv " + memberList.getMyNodeId() + " REFUSE " + proposal.getCommand() + " last seen prop id = " + thisInstance.getLastPreparedPropId() + " this prop id = " + proposal.getId());
                //System.out.println("acceptor " + memberList.getMyNodeId() + " inst=" + instanceNb + " accept NOK cmd=" + proposal.getCommand() + " propId=" + proposal.getId());
                return false;
            } else {
                thisInstance.setLastAcceptedProp(proposal);
                Logger.println("--- inst " + instanceNb + " srv " + memberList.getMyNodeId() + " accept " + proposal.getCommand());
                //System.out.println("acceptor " + memberList.getMyNodeId() + " inst=" + instanceNb + " accept OK cmd=" + proposal.getCommand() + " propId=" + proposal.getId());
                return true;
            }
        }
    }

    @Override
    public long getLastInstance() {
        return instances.getHighestInstance();
    }
}
