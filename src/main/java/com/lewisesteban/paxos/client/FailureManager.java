package com.lewisesteban.paxos.client;

import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import java.io.*;

class FailureManager {
    private static final String KEY_HASH = "h";
    private static final String KEY_CMD = "c";
    private static final String KEY_INST = "i";
    private static final String KEY_NB = "n";

    private String fileName;
    private StorageUnit.Creator storage;
    private Serializable ongoingCmdData;
    private int ongoingCmdKeyHash;
    private long ongoingCmdNb;

    FailureManager(StorageUnit.Creator storageCreator, String clientId) {
        fileName = "clientOngoingCommand_" + clientId;
        this.storage = storageCreator;
    }

    ClientOperation getLastStartedOperation() throws StorageException {
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
        clientOperation.cmdNb = Long.parseLong(file.read(KEY_NB));
        return clientOperation;
    }

    void setOngoingCmdData(Serializable cmdData) {
        this.ongoingCmdData = cmdData;
    }

    void setOngoingCmdKeyHash(int ongoingCmdKeyHash) {
        this.ongoingCmdKeyHash = ongoingCmdKeyHash;
    }

    void setOngoingCmdNb(long ongoingCmdNb) {
        this.ongoingCmdNb = ongoingCmdNb;
    }

    /**
     * Called directly before a propose() on a new instance, after the key hash, cmd data and cmd number have been set
     * properly.
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
        file.put(KEY_NB, Long.toString(ongoingCmdNb));
        file.flush();
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
        private long cmdNb;
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

        long getCmdNb() {
            return cmdNb;
        }
    }
}
