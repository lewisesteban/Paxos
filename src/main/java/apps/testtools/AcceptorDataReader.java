package apps.testtools;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.storage.*;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.LinkedList;
import java.util.List;

public class AcceptorDataReader {
    private static final String STORAGE_KEY_LAST_ACCEPTED_ID_NODE = "c";
    private static final String STORAGE_KEY_LAST_ACCEPTED_ID_PROP = "d";
    private static final String STORAGE_KEY_LAST_ACCEPTED_CMD = "e";

    public static void main(String ... args) throws StorageException {
        readStorage(WholeFileAccessor::new, (f, dir) -> new SafeSingleFileStorage(f, dir, WholeFileAccessor::new));
    }

    private static void readStorage(FileAccessorCreator fileAccessorCreator, StorageUnit.Creator storageUnitCreator) throws StorageException {
        List<Long> list = new LinkedList<>();
        FileAccessor folder = fileAccessorCreator.create(".", null);
        FileAccessor[] files = folder.listFiles();
        if (files != null) {
            for (FileAccessor file : files) {
                String name = file.getName();
                if (name.endsWith("_tmp"))
                    name = name.substring(0, name.indexOf("_tmp"));
                long instance = Long.parseLong(name.substring("inst".length()));
                if (!list.contains(instance)) {
                    StorageUnit storageUnit = storageUnitCreator.make("inst" + instance, null);
                    if (!storageUnit.isEmpty()) {
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
                        if (lastAcceptedProp != null) {
                            System.out.println("inst=" + instance
                                    + "\tclient=" + lastAcceptedProp.getCommand().getClientId()
                                    + "\tcmdNb=" + lastAcceptedProp.getCommand().getClientCmdNb()
                                    + "\tnoOp=" + lastAcceptedProp.getCommand().isNoOp()
                                    + "\tcmd=" + (lastAcceptedProp.getCommand().isNoOp() ? "N/A" : lastAcceptedProp.getCommand().getData()));
                        }
                        list.add(instance);
                        storageUnit.close();
                    }
                }
            }
        }
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
