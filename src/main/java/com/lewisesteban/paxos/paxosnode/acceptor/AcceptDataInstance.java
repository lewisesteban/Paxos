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
                storage.put(STORAGE_KEY_LAST_ACCEPTED_CMD_DATA, serializeCommandData(lastAcceptedProp.getCommand().getData()));
            } catch (IOException e) {
                throw new StorageException(e);
            }
            storage.put(STORAGE_KEY_LAST_ACCEPTED_CMD_CLIENT, lastAcceptedProp.getCommand().getClientId());
            storage.put(STORAGE_KEY_LAST_ACCEPTED_CMD_NB, String.valueOf(lastAcceptedProp.getCommand().getClientCmdNb()));
        }
        try {
            storage.flush();
            storage.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static Map<Long, AcceptDataInstance> readStorage(int nodeId, FileAccessorCreator fileAccessorCreator, StorageUnit.Creator storageUnitCreator) throws StorageException {
        Map<Long, AcceptDataInstance> list = new TreeMap<>();
        FileAccessor folder = fileAccessorCreator.create("acceptor" + nodeId, null);
        FileAccessor[] matchingFiles = folder.listFiles();
        if (matchingFiles != null) {
            for (FileAccessor file : matchingFiles) {
                if (file.getName().startsWith("inst") && !file.getName().endsWith("tmp")) {
                    Long instance = Long.parseLong(file.getName().substring(("inst").length()));
                    StorageUnit storageUnit = storageUnitCreator.make(file.getName(), "acceptor" + nodeId);
                    int lastPreparedPropId_node = Integer.parseInt(storageUnit.read(STORAGE_KEY_LAST_PREPARED_ID_NODE));
                    int lastPreparedPropId_prop = Integer.parseInt(storageUnit.read(STORAGE_KEY_LAST_PREPARED_ID_PROP));
                    Proposal.ID lastPreparedPropId = new Proposal.ID(lastPreparedPropId_node, lastPreparedPropId_prop);
                    Proposal lastAcceptedProp = null;
                    if (storageUnit.read(STORAGE_KEY_LAST_ACCEPTED_ID_NODE) != null) {
                        int lastAcceptedId_node = Integer.parseInt(storageUnit.read(STORAGE_KEY_LAST_ACCEPTED_ID_NODE));
                        int lastAcceptedId_prop = Integer.parseInt(storageUnit.read(STORAGE_KEY_LAST_ACCEPTED_ID_PROP));
                        Serializable lastAcceptedCmd_data;
                        try {
                            lastAcceptedCmd_data = deserializeCommandData(storageUnit.read(STORAGE_KEY_LAST_ACCEPTED_CMD_DATA));
                        } catch (IOException e) {
                            throw new StorageException(e);
                        }
                        String lastAcceptedCmd_client = storageUnit.read(STORAGE_KEY_LAST_ACCEPTED_CMD_CLIENT);
                        long lastAcceptedCmd_nb = Long.parseLong(storageUnit.read(STORAGE_KEY_LAST_ACCEPTED_CMD_NB));
                        Command lastAcceptedCmd = new Command(lastAcceptedCmd_data, lastAcceptedCmd_client, lastAcceptedCmd_nb);
                        lastAcceptedProp = new Proposal(lastAcceptedCmd, new Proposal.ID(lastAcceptedId_node, lastAcceptedId_prop));
                    }
                    list.put(instance, new AcceptDataInstance(lastPreparedPropId, lastAcceptedProp));
                    storageUnit.close();
                }
            }
        }
        return list;
    }

    private static String serializeCommandData(Serializable data) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
        objectOutputStream.writeObject(data);
        objectOutputStream.flush();
        return Base64.encode(outputStream.toByteArray());
    }

    private static Serializable deserializeCommandData(String serialized) throws IOException {
        byte[] bytes = Base64.decode(serialized);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        try {
            return (Serializable)objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new StorageException(e);
        }
    }
}
