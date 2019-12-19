package apps;

import com.lewisesteban.paxos.paxosnode.PaxosNode;
import com.lewisesteban.paxos.storage.SafeSingleFileStorage;
import com.lewisesteban.paxos.storage.WholeFileAccessor;
import network.NodeServer;

public class ServerLauncher {
    public static void main(String[] args) {
        if (!checkArgs(args)) {
            return;
        }

        try {
            // LocateRegistry.createRegistry(1099);
            // "rmiregistry" should be started before launching the program, and in the root folder containing compiled
            // files. In IntelliJ, that would be "target/classes".

            final int serverId = Integer.parseInt(args[1]);
            final int fragmentId = Integer.parseInt(args[0]);
            NodeServer nodeServer = NetworkFileParser.createServer(args.length > 2 ? args[2] : null,
                    serverId, fragmentId, cluster -> {
                        PaxosNode paxosNode = new PaxosNode(serverId, fragmentId, cluster,
                                new largetable.Server(WholeFileAccessor::new),
                                (f, dir) -> new SafeSingleFileStorage(f, dir, WholeFileAccessor::new),
                                WholeFileAccessor::new);
                        return new NodeServer(paxosNode);
                    });
            nodeServer.start();

            System.err.println("Server ready");
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
