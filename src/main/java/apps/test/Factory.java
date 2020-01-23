package apps.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

class Factory {
    private String clientsFile;
    private String serversFile;

    Factory(String clientsFile, String serversFile) {
        this.clientsFile = clientsFile;
        this.serversFile = serversFile;
    }

    List<TesterClient> createClients() throws IOException {
        List<TesterClient> clients = new ArrayList<>();
        File file = new File(clientsFile);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        while ((line = br.readLine()) != null) {
            String[] words = line.split(" ");
            if (words.length != 2) {
                throw new IOException("Each line of '" + clientsFile + "' should contain the host and the client name, separated by a space");
            }
            clients.add(new TesterClient(words[0], words[1]));
        }
        return clients;
    }

    List<TesterServer> createServers() throws IOException {
        List<TesterServer> servers = new ArrayList<>();
        Map<Integer, Integer> fragmentIndices = new TreeMap<>();
        File file = new File(serversFile);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        while ((line = br.readLine()) != null) {
            String[] words = line.split(" ");
            if (words.length != 2) {
                throw new IOException("Each line of the file '" + serversFile + "' should contain the host and the fragment number, separated by a space. The index of the line is the index of the server in its fragment.");
            }
            int fragment;
            try {
                fragment = Integer.parseInt(words[1]);
            } catch (NumberFormatException e) {
                System.err.println("Each line of the file '" + serversFile + "' should contain the host and the fragment number, separated by a space. The index of the line is the index of the server in its fragment.");
                throw e;
            }
            int srvIndex = fragmentIndices.getOrDefault(fragment, -1) + 1;
            fragmentIndices.put(fragment, srvIndex);
            servers.add(new TesterServer(words[0], srvIndex, fragment));
        }
        return servers;
    }
}
