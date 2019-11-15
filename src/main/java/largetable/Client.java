package largetable;

import com.lewisesteban.paxos.client.PaxosClient;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.storage.FileAccessorCreator;
import com.lewisesteban.paxos.storage.SafeSingleFileStorage;
import com.lewisesteban.paxos.storage.StorageException;

import java.util.List;

public class Client<NODE extends PaxosProposer & RemotePaxosNode> {
    private PaxosClient<NODE> paxosClient;

    public Client(List<NODE> allNodes, String clientId, FileAccessorCreator fileAccessorCreator) throws StorageException {
        paxosClient = new PaxosClient<>(allNodes, clientId,
                (name, dir) -> new SafeSingleFileStorage(name, dir, fileAccessorCreator));
        paxosClient.recover();
    }

    public String get(String key) {
        return doCommand(new Command(Command.GET, new String[] { key }));
    }

    public void put(String key, String value) {
        doCommand(new Command(Command.PUT, new String[] { key, value }));
    }

    public void append(String key, String value) {
        doCommand(new Command(Command.APPEND, new String[] { key, value }));
    }

    public void close() {
        paxosClient.end();
    }

    private String doCommand(Command command) {
        return (String) paxosClient.doCommand(command, command.getData()[0].hashCode());
    }
}
