package largetable;

import com.lewisesteban.paxos.client.PaxosClient;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;

import java.util.List;

// TODO failure management: higher in hierarchy, with instance id and all (have  a start method in client, that checks for file)
public class Client<NODE extends PaxosProposer & RemotePaxosNode> {
    private PaxosClient<NODE> paxosClient;

    public Client(List<NODE> allNodes, String clientId) {
        paxosClient = new PaxosClient<>(allNodes, clientId);
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

    private String doCommand(Command command) {
        return (String) paxosClient.doCommand(command, command.getData()[0].hashCode());
    }
}
