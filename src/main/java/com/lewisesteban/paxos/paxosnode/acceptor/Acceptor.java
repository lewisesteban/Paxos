package com.lewisesteban.paxos.paxosnode.acceptor;

import com.lewisesteban.paxos.paxosnode.MembershipGetter;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;
import com.lewisesteban.paxos.storage.FileAccessorCreator;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;

public class Acceptor implements AcceptorRPCHandle {

    private InstanceContainer<AcceptDataInstance> instances;
    private MembershipGetter memberList;
    private StorageUnit.Creator storageCreator;

    public Acceptor(MembershipGetter memberList, StorageUnit.Creator storageUnitCreator, FileAccessorCreator fileAccessorCreator) throws StorageException {
        this.memberList = memberList;
        this.storageCreator = storageUnitCreator;
        this.instances = new InstanceContainer<>(AcceptDataInstance::new,
                AcceptDataInstance.readStorage(memberList.getMyNodeId(), fileAccessorCreator, storageUnitCreator));
    }

    public PrepareAnswer reqPrepare(long instanceNb, Proposal.ID propId) throws StorageException {
        AcceptDataInstance thisInstance = instances.get(instanceNb);
        synchronized (thisInstance) {
            if (propId.isGreaterThan(thisInstance.getLastPreparedPropId())) {
                thisInstance.setLastPreparedPropId(propId);
                thisInstance.saveToStorage(memberList.getMyNodeId(), instanceNb, storageCreator);
                return new PrepareAnswer(true, thisInstance.getLastAcceptedProp());
            } else {
                return new PrepareAnswer(false, null);
            }
        }
    }

    public boolean reqAccept(long instanceNb, Proposal proposal) throws StorageException {
        AcceptDataInstance thisInstance = instances.get(instanceNb);
        synchronized (thisInstance) {
            if (thisInstance.getLastPreparedPropId().isGreaterThan(proposal.getId())) {
                return false;
            } else {
                thisInstance.setLastAcceptedProp(proposal);
                thisInstance.saveToStorage(memberList.getMyNodeId(), instanceNb, storageCreator);
                return true;
            }
        }
    }

    @Override
    public long getLastInstance() {
        return instances.getHighestInstance();
    }
}
