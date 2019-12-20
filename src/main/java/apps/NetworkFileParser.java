package apps;

import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.storage.StorageException;
import network.NodeClient;
import network.NodeServer;

import java.io.*;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

class NetworkFileParser {

    static void createRemoteNodes(String filePath, NodeServer server, List<RemotePaxosNode> cluster) throws RemoteException, NotBoundException, StorageException, FileFormatException, FileNotFoundException {
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
                cluster.add(new NodeClient(host, nodeId, server.getFragmentId()));
            }

        }
    }

    static List<NodeClient> createRemoteNodes(String filePath) throws NotBoundException, StorageException, FileFormatException, RemoteException, FileNotFoundException {
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
