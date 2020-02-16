package com.lewisesteban.paxos.paxosnode.acceptor;

import com.lewisesteban.paxos.paxosnode.ClusterHandle;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;
import com.lewisesteban.paxos.storage.FileAccessorCreator;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class Acceptor implements AcceptorRPCHandle {

    private InstanceContainer<AcceptDataInstance> instances;
    private ClusterHandle memberList;
    private StorageUnit.Creator storageCreator;

    public Acceptor(ClusterHandle memberList, StorageUnit.Creator storageUnitCreator, FileAccessorCreator fileAccessorCreator) throws StorageException {
        this.memberList = memberList;
        this.storageCreator = storageUnitCreator;
        this.instances = new InstanceContainer<>(AcceptDataInstance::new,
                AcceptDataInstance.readStorage(memberList.getMyNodeId(), fileAccessorCreator, storageUnitCreator));
    }

    public PrepareAnswer reqPrepare(long instanceNb, Proposal.ID propId) throws StorageException {
        AcceptDataInstance thisInstance = instances.get(instanceNb);
        if (thisInstance == null) {
            return new PrepareAnswer(false, null, true);
        }
        synchronized (thisInstance) {
            if (propId.isGreaterThan(thisInstance.getLastPreparedPropId())) {
                thisInstance.setLastPreparedPropId(propId);
                thisInstance.saveToStorage(memberList.getMyNodeId(), instanceNb, storageCreator);
                return new PrepareAnswer(true, thisInstance.getLastAcceptedProp());
            } else {
                return new PrepareAnswer(false, null, thisInstance.getLastPreparedPropId().getNodePropNb());
            }
        }
    }

    public AcceptAnswer reqAccept(long instanceNb, Proposal proposal) throws StorageException {
        AcceptDataInstance thisInstance = instances.get(instanceNb);
        if (thisInstance == null)
            return new AcceptAnswer(AcceptAnswer.SNAPSHOT_REQUEST_REQUIRED);
        synchronized (thisInstance) {
            if (thisInstance.getLastPreparedPropId().isGreaterThan(proposal.getId())) {
                return new AcceptAnswer(AcceptAnswer.REFUSED);
            } else {
                thisInstance.setLastAcceptedProp(proposal);
                thisInstance.saveToStorage(memberList.getMyNodeId(), instanceNb, storageCreator);
                return new AcceptAnswer(AcceptAnswer.ACCEPTED);
            }
        }
    }

    @Override
    public PrepareAnswer[] bulkPrepare(long[] instanceIds, Proposal.ID[] propIds) throws IOException {
        PrepareAnswer[] answers = new PrepareAnswer[instanceIds.length];
        for (int i = 0; i < instanceIds.length; ++i) {
            if (instanceIds[i] < 0)
                answers[i] = null;
            else
                answers[i] = reqPrepare(instanceIds[i], propIds[i]);
        }
        return answers;
    }

    @Override
    public AcceptAnswer[] bulkAccept(long[] instanceIds, Proposal[] proposals) throws IOException {
        AcceptAnswer[] answers = new AcceptAnswer[instanceIds.length];
        for (int i = 0; i < instanceIds.length; ++i) {
            if (instanceIds[i] < 0)
                answers[i] = null;
            else
                answers[i] = reqAccept(instanceIds[i], proposals[i]);
        }
        return answers;
    }

    @Override
    public long getLastInstance() {
        return instances.getHighestInstance();
    }

    @Override
    public long getLastPropNb() {
        if (instances.getHighestInstance() == -1)
            return 0;
        return instances.get(instances.getHighestInstance()).getLastPreparedPropId().getNodePropNb();
    }

    public void removeLogsUntil(long lastInstanceToRemove) throws StorageException {
        AtomicReference<StorageException> exception = new AtomicReference<>(null);
        instances.truncateBefore(lastInstanceToRemove + 1,
                (entry) -> {
                    try {
                        entry.getValue().deleteStorage(memberList.getMyNodeId(), entry.getKey(), storageCreator);
                    } catch (StorageException e) {
                        exception.set(e);
                    }
                });
        if (exception.get() != null)
            throw exception.get();
    }
}
