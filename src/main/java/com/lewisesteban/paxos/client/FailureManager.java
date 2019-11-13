package com.lewisesteban.paxos.client;

import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import java.io.*;

class FailureManager {
    private static final String KEY_HASH = "h";
    private static final String KEY_CMD = "c";
    private static final String KEY_INST = "i";
    private String fileName;
    private StorageUnit.Creator storage;
    private Serializable ongoingCmdData;
    private int ongoingCmdKeyHash;

    FailureManager(StorageUnit.Creator storageCreator, String clientId) {
        fileName = "clientOngoingCommand_" + clientId;
        this.storage = storageCreator;
    }

    ClientOperation getFailedOperation() throws StorageException {
        StorageUnit file = storage.make(fileName, null);
        if (file.isEmpty() || file.read(KEY_INST) == null)
            return null;
        ClientOperation clientOperation = new ClientOperation();
        clientOperation.keyHash = Integer.parseInt(file.read(KEY_HASH));
        try {
            clientOperation.cmdData = deserializeData(file.read(KEY_CMD));
        } catch (IOException e) {
            throw new StorageException(e);
        }
        clientOperation.inst = Long.parseLong(file.read(KEY_INST));
        return clientOperation;
    }

    /**
     * Called when starting a new command on the client (before startInstance).
     */
    void startCommand(Serializable cmdData, int keyHash) {
        this.ongoingCmdData = cmdData;
        this.ongoingCmdKeyHash = keyHash;
    }

    /**
     * Called directly before a propose() on a new instance
     */
    void startInstance(long instance) throws StorageException {
        StorageUnit file = storage.make(fileName, null).overwriteMode();
        file.put(KEY_INST, Long.toString(instance));
        try {
            file.put(KEY_CMD, serializeData(ongoingCmdData));
        } catch (IOException e) {
            throw new StorageException(e);
        }
        file.put(KEY_HASH, Integer.toString(ongoingCmdKeyHash));
        file.flush();
    }

    /**
     * Called after receiving a positive response from Paxos
     */
    void endCommand() {
        try {
            storage.make(fileName, null).delete();
        } catch (StorageException e) {
            e.printStackTrace();
        }
    }

    private String serializeData(Serializable data) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
        objectOutputStream.writeObject(data);
        objectOutputStream.flush();
        return Base64.encode(outputStream.toByteArray());
    }

    private Serializable deserializeData(String serialized) throws IOException {
        byte[] bytes = Base64.decode(serialized);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        try {
            return (Serializable) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new StorageException(e);
        }
    }

    class ClientOperation {
        private int keyHash;
        private Serializable cmdData;
        private Long inst;

        int getKeyHash() {
            return keyHash;
        }

        Serializable getCmdData() {
            return cmdData;
        }

        Long getInst() {
            return inst;
        }
    }
}
