package com.lewisesteban.paxos.paxosnode.proposer;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.storage.FileAccessor;
import com.lewisesteban.paxos.storage.FileAccessorCreator;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import java.io.*;
import java.util.Map;
import java.util.TreeMap;

@SuppressWarnings("UnusedReturnValue")
public class ClientCommandContainer {
    public static int TIMEOUT = 1000 * 60 * 60 * 2;
    public static int TIMEOUT_CHECK_FREQUENCY = 1000;

    private Map<String, ClientCommand> clientCommands = new TreeMap<>();
    private StorageUnit.Creator storageCreator;
    private long lastTimeoutCheck = 0;
    private int nodeId;

    public ClientCommandContainer(StorageUnit.Creator storageCreator, FileAccessorCreator fileAccessorCreator, int nodeId)
            throws StorageException {
        this.storageCreator = storageCreator;
        this.nodeId = nodeId;

        FileAccessor dir = fileAccessorCreator.create("commandManager" + nodeId, null);
        if (dir.exists()) {
            FileAccessor[] files = dir.listFiles();
            if (files != null) {
                for (FileAccessor file : files) {
                    String clientId = file.getName();
                    if (clientId.endsWith("_tmp"))
                        clientId = clientId.substring(0, clientId.length() - 4);
                    if (!clientCommands.containsKey(clientId)) {
                        ClientCommand clientCommand = new ClientCommand(clientId);
                        clientCommands.put(clientId, clientCommand);
                    }
                }
            }
        }
    }

    synchronized boolean putCommand(Command command, long instance) throws StorageException {
        ClientCommand clientCommand = clientCommands.get(command.getClientId());
        if (clientCommand == null) {
            clientCommand = new ClientCommand(command.getClientId());
            clientCommands.put(command.getClientId(), clientCommand);
        }
        clientCommand.setCommand(command, instance);
        return true;
    }

    synchronized boolean deleteClient(String clientId) throws StorageException {
        ClientCommand cmd = getCommand(clientId);
        if (cmd == null)
            return false;
        cmd.delete();
        clientCommands.remove(clientId);
        return true;
    }

    private synchronized ClientCommand getCommand(String clientId) {
        return clientCommands.get(clientId);
    }

    private synchronized void checkTimeouts() {
        if (System.currentTimeMillis() - lastTimeoutCheck > TIMEOUT_CHECK_FREQUENCY) {
            clientCommands.forEach((key, value) -> {
                if (System.currentTimeMillis() - value.timestamp > TIMEOUT) {
                    try {
                        value.delete();
                    } catch (StorageException ignored) {
                    }
                }
            });
            clientCommands.entrySet().removeIf(entry -> entry.getValue().getDeleted());
            lastTimeoutCheck = System.currentTimeMillis();
        }
    }

    public synchronized Long getLowestInstanceId() {
        checkTimeouts();
        Long lowest = null;
        for (ClientCommand clientCommand : clientCommands.values()) {
            if (lowest == null || clientCommand.getInstance() < lowest) {
                lowest = clientCommand.instance;
            }
        }
        return lowest;
    }

    class ClientCommand {
        private final String KEY_INSTANCE = "i";
        private final String KEY_CMD_NB = "n";
        private final String KEY_CMD_DATA = "d";

        private Command command = null;
        private long instance = -1;
        private long timestamp;
        private StorageUnit storageUnit;
        private boolean isDeleted = false;

        private ClientCommand(String clientId) throws StorageException {
            this.storageUnit = storageCreator.make(clientId, "commandManager" + nodeId);
            if (!storageUnit.isEmpty()) {
                this.instance = Long.parseLong(storageUnit.read(KEY_INSTANCE));
                long cmdNb = Long.parseLong(storageUnit.read(KEY_CMD_NB));
                Serializable cmdData;
                try {
                    cmdData = deserializeCommandData(storageUnit.read(KEY_CMD_DATA));
                } catch (IOException e) {
                    throw new StorageException(e);
                }
                this.command = new Command(cmdData, clientId, cmdNb);
            }
            this.timestamp = System.currentTimeMillis();
        }

        public Command getCommand() {
            return command;
        }

        public long getInstance() {
            return instance;
        }

        private void setInstance(long instance) throws StorageException {
            this.instance = instance;
            this.timestamp = System.currentTimeMillis();
            storageUnit.put(KEY_INSTANCE, Long.toString(instance));
            storageUnit.flush();
        }

        private void setCommand(Command command, long instance) throws StorageException {
            if (this.command != null && this.command.equals(command)) {
                setInstance(instance);
            } else {
                this.instance = instance;
                this.command = command;
                this.timestamp = System.currentTimeMillis();
                storageUnit.put(KEY_INSTANCE, Long.toString(instance));
                storageUnit.put(KEY_CMD_NB, Long.toString(command.getClientCmdNb()));
                try {
                    storageUnit.put(KEY_CMD_DATA, serializeCommandData());
                } catch (IOException e) {
                    throw new StorageException(e);
                }
                storageUnit.flush();
            }
        }

        private void delete() throws StorageException {
            storageUnit.delete();
            isDeleted = true;
        }

        boolean getDeleted() {
            return isDeleted;
        }

        private String serializeCommandData() throws StorageException {
            try {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                objectOutputStream.writeObject(command.getData());
                objectOutputStream.flush();
                return Base64.encode(outputStream.toByteArray());
            } catch (IOException e) {
                throw new StorageException(e);
            }
        }

        private Serializable deserializeCommandData(String serialized) throws StorageException {
            try {
                byte[] bytes = Base64.decode(serialized);
                ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
                ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                return (Serializable) objectInputStream.readObject();
            } catch (IOException | ClassNotFoundException e) {
                throw new StorageException(e);
            }
        }
    }
}