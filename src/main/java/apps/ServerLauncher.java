package apps;

import com.lewisesteban.paxos.paxosnode.PaxosNode;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.storage.SafeSingleFileStorage;
import com.lewisesteban.paxos.storage.WholeFileAccessor;
import network.MultiClientCatchingUpManager;
import network.NodeServer;

import java.util.ArrayList;
import java.util.List;

public class ServerLauncher {
    public static void main(String[] args) {
        if (!checkArgs(args)) {
            return;
        }

        try {
            // "rmiregistry" should be started before launching the program, and in the root folder containing compiled
            // files. In IntelliJ, that would be "target/classes".
            // It may also be created at runtime like this:
            // LocateRegistry.createRegistry(1099);

            final int serverId = Integer.parseInt(args[1]);
            final int fragmentId = Integer.parseInt(args[0]);
            List<RemotePaxosNode> cluster = new ArrayList<>();
            PaxosNode paxosNode = new PaxosNode(serverId, fragmentId, cluster,
                    new largetable.Server(WholeFileAccessor::new),
                    (f, dir) -> new SafeSingleFileStorage(f, dir, WholeFileAccessor::new),
                    WholeFileAccessor::new);
            NodeServer server = new NodeServer(paxosNode);

            List<MultiClientCatchingUpManager.ClientCUMGetter> catchingUpManagers = new ArrayList<>();
            NetworkFileParser.createRemoteNodes(args.length == 3 ? args[2] : null, server, cluster, catchingUpManagers);
            paxosNode.setCatchingUpManager(new MultiClientCatchingUpManager(catchingUpManagers));
            server.start();
            System.out.println("Server ready.");

        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
    }

    private static boolean checkArgs(String[] args) {
        if (!(args.length == 2 || args.length == 3)) {
            System.err.println("Expected arguments: fragmentID nodeID");
            return false;
        }
        if (!args[0].matches("\\d+")) {
            System.err.println("The first argument (fragmentID) is incorrect. Should be a number.");
            return false;
        }
        if (!args[1].matches("\\d+")) {
            System.err.println("The second argument (nodeId) is incorrect. Should be a number.");
            return false;
        }
        return true;
    }
}
