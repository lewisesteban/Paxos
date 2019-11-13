package largetable;

import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.storage.FileAccessorCreator;
import com.lewisesteban.paxos.storage.SafeSingleFileStorage;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import java.io.*;
import java.util.TreeMap;
import java.util.concurrent.Callable;

public class Server implements StateMachine {
    private static final String KEY_INST = "i";
    private static final String KEY_DATA = "d";

    private FileAccessorCreator fileAccessorCreator;
    private int nodeId;
    private Long appliedSnapshotLastInstance = null;
    private Snapshot waitingSnapshot = null;
    private TreeMap<String, String> table = new TreeMap<>();

    public Server(FileAccessorCreator fileAccessorCreator) {
        this.fileAccessorCreator = fileAccessorCreator;
    }

    @Override
    public void setup(int nodeId) throws IOException {
        this.nodeId = nodeId;
        StorageUnit storageUnit = createStorage();
        if (!storageUnit.isEmpty()) {
            long inst = Long.parseLong(storageUnit.read(KEY_INST));
            TreeMap<String, String> data = deserializeData(storageUnit.read(KEY_DATA));
            applySnapshot(new Snapshot(inst, data));
        }
    }

    private StorageUnit createStorage() throws StorageException {
        return new SafeSingleFileStorage("stateMachine" + nodeId, null, fileAccessorCreator);
    }

    @Override
    public Serializable execute(Serializable data) {
        return ((Command) data).apply(table);
    }

    @Override
    public void createWaitingSnapshot(long idOfLastExecutedInstance) {
        try {
            waitingSnapshot = new Snapshot(idOfLastExecutedInstance, serializeData(table));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Snapshot getAppliedSnapshot() throws StorageException {
        StorageUnit storageUnit = createStorage();
        if (!storageUnit.isEmpty()) {
            long inst = Long.parseLong(storageUnit.read(KEY_INST));
            TreeMap<String, String> data;
            try {
                data = deserializeData(storageUnit.read(KEY_DATA));
            } catch (IOException e) {
                throw new StorageException(e);
            }
            return new Snapshot(inst, data);
        }
        return null;
    }

    @Override
    public long getWaitingSnapshotLastInstance() {
        return waitingSnapshot == null ? -1 : waitingSnapshot.getLastIncludedInstance();
    }

    @Override
    public long getAppliedSnapshotLastInstance() {
        return appliedSnapshotLastInstance == null ? -1 : appliedSnapshotLastInstance;
    }

    @Override
    public void applyCurrentWaitingSnapshot() throws StorageException {
        StorageUnit storageUnit = createStorage().overwriteMode();
        storageUnit.put(KEY_INST, Long.toString(waitingSnapshot.getLastIncludedInstance()));
        try {
            //noinspection unchecked
            storageUnit.put(KEY_DATA, serializeData((TreeMap<String, String>) waitingSnapshot.getData()));
        } catch (IOException e) {
            throw new StorageException(e);
        }
        storageUnit.flush();
        appliedSnapshotLastInstance = waitingSnapshot.getLastIncludedInstance();
        waitingSnapshot = null;
    }

    @Override
    public void applySnapshot(Snapshot snapshot) throws StorageException {
        StorageUnit storageUnit = createStorage().overwriteMode();
        //noinspection unchecked
        TreeMap<String, String> snapshotData = (TreeMap<String, String>) snapshot.getData();

        storageUnit.put(KEY_INST, Long.toString(snapshot.getLastIncludedInstance()));
        try {
            storageUnit.put(KEY_DATA, serializeData(snapshotData));
        } catch (IOException e) {
            throw new StorageException(e);
        }
        storageUnit.flush();

        table = snapshotData;
        appliedSnapshotLastInstance = snapshot.getLastIncludedInstance();
        waitingSnapshot = null;
    }

    @Override
    public boolean hasWaitingSnapshot() {
        return waitingSnapshot != null;
    }

    @Override
    public boolean hasAppliedSnapshot() {
        return appliedSnapshotLastInstance != null;
    }

    private String serializeData(TreeMap<String, String> data) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
        objectOutputStream.writeObject(data);
        objectOutputStream.flush();
        return Base64.encode(outputStream.toByteArray());
    }

    private TreeMap<String, String> deserializeData(String serialized) throws IOException {
        byte[] bytes = Base64.decode(serialized);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        try {
            //noinspection unchecked
            return (TreeMap<String, String>)objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new StorageException(e);
        }
    }

    public static Callable<StateMachine> creator(FileAccessorCreator fileAccessorCreator) {
        return () -> new Server(fileAccessorCreator);
    }
}
