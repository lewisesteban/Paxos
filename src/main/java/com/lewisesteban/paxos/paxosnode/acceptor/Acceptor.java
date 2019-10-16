package com.lewisesteban.paxos.paxosnode.acceptor;

import com.lewisesteban.paxos.Logger;
import com.lewisesteban.paxos.paxosnode.MembershipGetter;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;
import com.lewisesteban.paxos.storage.FileAccessorCreator;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;

public class Acceptor implements AcceptorRPCHandle {

    private InstanceContainer<AcceptDataInstance> instances;
    private MembershipGetter memberList;
    private FileAccessorCreator fileAccessorCreator;

    public Acceptor(MembershipGetter memberList, StorageUnit.Creator storageUnitCreator, FileAccessorCreator fileAccessorCreator) throws StorageException {
        this.memberList = memberList;
        this.fileAccessorCreator = fileAccessorCreator;
        this.instances = new InstanceContainer<>(AcceptDataInstance::new,
                AcceptDataInstance.readStorage(memberList.getMyNodeId(), storageUnitCreator));
    }

    public PrepareAnswer reqPrepare(long instanceNb, Proposal.ID propId) throws StorageException {
        AcceptDataInstance thisInstance = instances.get(instanceNb);
        synchronized (thisInstance) {
            if (propId.isGreaterThan(thisInstance.getLastPreparedPropId())) {
                thisInstance.setLastPreparedPropId(propId);
                thisInstance.saveToStorage(memberList.getMyNodeId(), instanceNb, fileAccessorCreator);
                Logger.println("--o inst " + instanceNb + " srv " + memberList.getMyNodeId() + " prepare OK " + propId);
                return new PrepareAnswer(true, thisInstance.getLastAcceptedProp());
            } else {
                Logger.println("--o inst " + instanceNb + " srv " + memberList.getMyNodeId() + " prepare NOK " + propId);
                return new PrepareAnswer(false, null);
            }
        }
    }

    public boolean reqAccept(long instanceNb, Proposal proposal) throws StorageException {
        AcceptDataInstance thisInstance = instances.get(instanceNb);
        synchronized (thisInstance) {
            if (thisInstance.getLastPreparedPropId().isGreaterThan(proposal.getId())) {
                Logger.println("--o inst " + instanceNb + " srv " + memberList.getMyNodeId() + " REFUSE " + proposal.getCommand() + " last seen prop id = " + thisInstance.getLastPreparedPropId() + " this prop id = " + proposal.getId());
                return false;
            } else {
                thisInstance.setLastAcceptedProp(proposal);
                thisInstance.saveToStorage(memberList.getMyNodeId(), instanceNb, fileAccessorCreator);
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
