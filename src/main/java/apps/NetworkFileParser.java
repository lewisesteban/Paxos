package apps;

import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.storage.StorageException;
import network.MultiClientCatchingUpManager;
import network.NodeClient;
import network.NodeServer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

class NetworkFileParser {

    /**
     * Parses a file to create the objects that a Paxos server uses to connect to other servers.
     *
     * @param filePath File to parse, containing addresses of all servers of the cluster in the correct order.
     * @param server This node.
     * @param cluster A list that the node uses and needs to contain connectors to all nodes of the cluster.
     * @param catchingUpManagers An empty list of catching up managers, to be filled by this method.
     */
    static void createRemoteNodes(String filePath, NodeServer server, List<RemotePaxosNode> cluster, List<MultiClientCatchingUpManager.ClientCUMGetter> catchingUpManagers) throws StorageException, FileFormatException, FileNotFoundException {
        // read the network file
        File file = new File(getFilePath(filePath));
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        while (true) {
            try {
                if ((line = br.readLine()) == null) break;
            } catch (IOException e) {
                throw new StorageException(e);
            }

            // parse the line
            String[] words = line.split(" ");
            if (words.length != 1 || words[0].isEmpty())
                throw new FileFormatException("Each line of the network file should contain the host of the server. The position of the line indicates the ID of the server within the fragment.");
            String host = words[0];
            int nodeId = cluster.size();

            // create the node (client or server)
            if (nodeId == server.getId()) {
                cluster.add(server);
            } else {
                NodeClient nodeClient = new NodeClient(host, nodeId, server.getFragmentId());
                cluster.add(nodeClient);
                catchingUpManagers.add(nodeClient.getCatchingUpManager());
            }

        }
    }

    /**
     * Parses a file to create the objects that a Paxos client uses to connect to all servers.
     *
     * @param filePath File to parse, containing addresses of all servers in the correct order, with their fragment ID.
     * @return Connectors to all servers.
     */
    static List<NodeClient> createRemoteNodes(String filePath) throws StorageException, FileFormatException, FileNotFoundException {
        Map<Integer, List<NodeClient>> fragments = new TreeMap<>();

            // read the network file
            File file = new File(getFilePath(filePath));
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            while (true) {
                try {
                    if ((line = br.readLine()) == null) break;
                } catch (IOException e) {
                    throw new StorageException(e);
                }

                // parse the line
                String[] words = line.split(" ");
                if (words.length != 2 || !words[1].matches("\\d+") || words[0].isEmpty())
                    throw new FileFormatException("Each line of the network file should contain the host followed the fragment number, separated by a space. The position of the line indicates the ID of the server within the fragment.");
                String host = words[0];
                int fragmentNb = Integer.parseInt(words[1]);
                if (!fragments.containsKey(fragmentNb))
                    fragments.put(fragmentNb, new ArrayList<>());
                int nodeId = fragments.get(fragmentNb).size();

                // create the node
                fragments.get(fragmentNb).add(new NodeClient(host, nodeId, fragmentNb));

            }

        // return all nodes of all fragments
        return fragments.values().stream().collect(ArrayList::new, List::addAll, List::addAll);
    }

    private static String getFilePath(String providedPath) {
        return providedPath == null ? "network" : providedPath;
    }
}
