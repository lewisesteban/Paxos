package com.lewisesteban.paxos.paxosnode.acceptor;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.storage.FileAccessor;
import com.lewisesteban.paxos.storage.FileAccessorCreator;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import java.io.*;
import java.util.Map;
import java.util.TreeMap;

/**
 * Not thread-safe
 */
class AcceptDataInstance implements Serializable {

    private static final String STORAGE_KEY_LAST_PREPARED_ID_NODE = "a";
    private static final String STORAGE_KEY_LAST_PREPARED_ID_PROP = "b";
    private static final String STORAGE_KEY_LAST_ACCEPTED_ID_NODE = "c";
    private static final String STORAGE_KEY_LAST_ACCEPTED_ID_PROP = "d";
    private static final String STORAGE_KEY_LAST_ACCEPTED_CMD = "e";

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

    // TODO don't write to storage if there has been no change
    void saveToStorage(int nodeId, long instanceNb, StorageUnit.Creator storageCreator) throws StorageException {
        StorageUnit storage = storageCreator.make("inst" + instanceNb, "acceptor" + nodeId);
        storage.put(STORAGE_KEY_LAST_PREPARED_ID_NODE, String.valueOf(lastPreparedPropId.getNodeId()));
        storage.put(STORAGE_KEY_LAST_PREPARED_ID_PROP, String.valueOf(lastPreparedPropId.getNodePropNb()));
        if (lastAcceptedProp == null) {
            storage.put(STORAGE_KEY_LAST_ACCEPTED_ID_NODE, null);
        } else {
            storage.put(STORAGE_KEY_LAST_ACCEPTED_ID_NODE, String.valueOf(lastAcceptedProp.getId().getNodeId()));
            storage.put(STORAGE_KEY_LAST_ACCEPTED_ID_PROP, String.valueOf(lastAcceptedProp.getId().getNodePropNb()));
            try {
                storage.put(STORAGE_KEY_LAST_ACCEPTED_CMD, serializeCommand(lastAcceptedProp.getCommand()));
            } catch (IOException e) {
                throw new StorageException(e);
            }
        }
        storage.flush();
        storage.close();
    }

    void deleteStorage(int nodeId, long instanceNb, StorageUnit.Creator storageCreator) throws StorageException {
        StorageUnit storage = storageCreator.make("inst" + instanceNb, "acceptor" + nodeId);
        storage.delete();
    }

    static Map<Long, AcceptDataInstance> readStorage(int nodeId, FileAccessorCreator fileAccessorCreator, StorageUnit.Creator storageUnitCreator) throws StorageException {
        Map<Long, AcceptDataInstance> list = new TreeMap<>();
        FileAccessor folder = fileAccessorCreator.create("acceptor" + nodeId, null);
        FileAccessor[] files = folder.listFiles();
        if (files != null) {
            for (FileAccessor file : files) {
                String name = file.getName();
                if (name.endsWith("_tmp"))
                    name = name.substring(0, name.indexOf("_tmp"));
                long instance = Long.parseLong(name.substring("inst".length()));
                if (!list.containsKey(instance)) {
                    StorageUnit storageUnit = storageUnitCreator.make("inst" + instance, "acceptor" + nodeId);
                    if (!storageUnit.isEmpty()) {
                        int lastPreparedPropId_node = Integer.parseInt(storageUnit.read(STORAGE_KEY_LAST_PREPARED_ID_NODE));
                        int lastPreparedPropId_prop = Integer.parseInt(storageUnit.read(STORAGE_KEY_LAST_PREPARED_ID_PROP));
                        Proposal.ID lastPreparedPropId = new Proposal.ID(lastPreparedPropId_node, lastPreparedPropId_prop);
                        Proposal lastAcceptedProp = null;
                        if (storageUnit.read(STORAGE_KEY_LAST_ACCEPTED_ID_NODE) != null) {
                            int lastAcceptedId_node = Integer.parseInt(storageUnit.read(STORAGE_KEY_LAST_ACCEPTED_ID_NODE));
                            int lastAcceptedId_prop = Integer.parseInt(storageUnit.read(STORAGE_KEY_LAST_ACCEPTED_ID_PROP));
                            Command lastAcceptedCmd;
                            try {
                                lastAcceptedCmd = deserializeCommand(storageUnit.read(STORAGE_KEY_LAST_ACCEPTED_CMD));
                            } catch (IOException e) {
                                throw new StorageException(e);
                            }
                            lastAcceptedProp = new Proposal(lastAcceptedCmd, new Proposal.ID(lastAcceptedId_node, lastAcceptedId_prop));
                        }
                        list.put(instance, new AcceptDataInstance(lastPreparedPropId, lastAcceptedProp));
                        storageUnit.close();
                    }
                }
            }
        }
        return list;
    }

    private static String serializeCommand(Command cmd) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
        objectOutputStream.writeObject(cmd);
        objectOutputStream.flush();
        return Base64.encode(outputStream.toByteArray());
    }

    private static Command deserializeCommand(String serialized) throws IOException {
        byte[] bytes = Base64.decode(serialized);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        try {
            return (Command) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new StorageException(e);
        }
    }
}
