package com.lewisesteban.paxos.paxosnode.acceptor;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;

import java.io.File;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

class AcceptDataInstance implements Serializable {

    private static final String STORAGE_KEY_LAST_PREPARED_ID_NODE = "a";
    private static final String STORAGE_KEY_LAST_PREPARED_ID_PROP = "b";
    private static final String STORAGE_KEY_LAST_ACCEPTED_ID_NODE = "c";
    private static final String STORAGE_KEY_LAST_ACCEPTED_ID_PROP = "d";
    private static final String STORAGE_KEY_LAST_ACCEPTED_CMD_DATA = "e";
    private static final String STORAGE_KEY_LAST_ACCEPTED_CMD_CLIENT = "f";
    private static final String STORAGE_KEY_LAST_ACCEPTED_CMD_NB = "g";

    private Proposal.ID lastPreparedPropId = Proposal.ID.noProposal();
    private Proposal lastAcceptedProp = null;

    AcceptDataInstance() { }

    private AcceptDataInstance(Proposal.ID lastPreparedPropId, Proposal lastAcceptedProp) {
        this.lastPreparedPropId = lastPreparedPropId;
        this.lastAcceptedProp = lastAcceptedProp;
    }

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

    void saveToStorage(int nodeId, long instanceNb, StorageUnit.Creator storageUnitCreator) throws StorageException {
        StorageUnit storageUnit = storageUnitCreator.make("inst" + instanceNb, "acceptor" + nodeId);
        storageUnit.put(STORAGE_KEY_LAST_PREPARED_ID_NODE, String.valueOf(lastPreparedPropId.getNodeId()));
        storageUnit.put(STORAGE_KEY_LAST_PREPARED_ID_PROP, String.valueOf(lastPreparedPropId.getNodePropNb()));
        if (lastAcceptedProp == null) {
            storageUnit.put(STORAGE_KEY_LAST_ACCEPTED_ID_NODE, null);
        } else {
            storageUnit.put(STORAGE_KEY_LAST_ACCEPTED_ID_NODE, String.valueOf(lastAcceptedProp.getId().getNodeId()));
            storageUnit.put(STORAGE_KEY_LAST_ACCEPTED_ID_PROP, String.valueOf(lastAcceptedProp.getId().getNodePropNb()));
            storageUnit.put(STORAGE_KEY_LAST_ACCEPTED_CMD_DATA, lastAcceptedProp.getCommand().getData().toString());
            storageUnit.put(STORAGE_KEY_LAST_ACCEPTED_CMD_CLIENT, lastAcceptedProp.getCommand().getClientId());
            storageUnit.put(STORAGE_KEY_LAST_ACCEPTED_CMD_NB, String.valueOf(lastAcceptedProp.getCommand().getClientCmdNb()));
        }
        storageUnit.flush();
    }

    static Map<Long, AcceptDataInstance> readStorage(int nodeId, StorageUnit.Creator storageUnitCreator) throws StorageException {
        Map<Long, AcceptDataInstance> list = new TreeMap<>();
        File folder = new File("acceptor" + nodeId);
        File[] matchingFiles = folder.listFiles((dir, name) -> name.startsWith("inst") && !name.endsWith("tmp"));
        if (matchingFiles != null) {
            for (File file : matchingFiles) {
                Long instance = Long.parseLong(file.getName().substring(("inst").length()));
                StorageUnit storageUnit = storageUnitCreator.make(file.getName(), "acceptor" + nodeId);
                int lastPreparedPropId_node = Integer.parseInt(storageUnit.read(STORAGE_KEY_LAST_PREPARED_ID_NODE));
                int lastPreparedPropId_prop = Integer.parseInt(storageUnit.read(STORAGE_KEY_LAST_PREPARED_ID_PROP));
                Proposal.ID lastPreparedPropId = new Proposal.ID(lastPreparedPropId_node, lastPreparedPropId_prop);
                Proposal lastAcceptedProp = null;
                if (storageUnit.read(STORAGE_KEY_LAST_ACCEPTED_ID_NODE) != null) {
                    int lastAcceptedId_node = Integer.parseInt(storageUnit.read(STORAGE_KEY_LAST_ACCEPTED_ID_NODE));
                    int lastAcceptedId_prop = Integer.parseInt(storageUnit.read(STORAGE_KEY_LAST_ACCEPTED_ID_PROP));
                    Serializable lastAcceptedCmd_data = storageUnit.read(STORAGE_KEY_LAST_ACCEPTED_CMD_DATA);
                    String lastAcceptedCmd_client = storageUnit.read(STORAGE_KEY_LAST_ACCEPTED_CMD_CLIENT);
                    long lastAcceptedCmd_nb = Long.parseLong(storageUnit.read(STORAGE_KEY_LAST_ACCEPTED_CMD_NB));
                    Command lastAcceptedCmd = new Command(lastAcceptedCmd_data, lastAcceptedCmd_client, lastAcceptedCmd_nb);
                    lastAcceptedProp = new Proposal(lastAcceptedCmd, new Proposal.ID(lastAcceptedId_node, lastAcceptedId_prop));
                }
                list.put(instance, new AcceptDataInstance(lastPreparedPropId, lastAcceptedProp));
                storageUnit.close();
            }
        }
        return list;
    }
}
