package apps;

import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.WholeFileAccessor;
import largetable.LargeTableClient;
import network.NodeClient;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

public class TerminalApp {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Expected arguments: clientId + (optional) network file");
            return;
        }
        try {
            readInputAndExecute(initialize(args));
            System.exit(0);
        } catch (StorageException | FileNotFoundException e) {
            System.err.println("ERR storage. " + e.getMessage().replace("\n", "").replace("\r", ""));
            System.exit(1);
        } catch (FileFormatException e) {
            System.err.println(e.getMessage());
        }
    }

    private static LargeTableClient initialize(String[] args) throws StorageException, FileNotFoundException, FileFormatException {
        String clientId = args[0];
        List<NodeClient> cluster = NetworkFileParser.createRemoteNodes(args.length <= 1 ? null : args[1]);
        return new LargeTableClient<>(cluster, clientId, WholeFileAccessor::new);
    }

    private static void readInputAndExecute(LargeTableClient client) {
        Interpreter interpreter = new Interpreter(client);
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            try {
                String line = reader.readLine();
                String res = interpreter.interpret(line);
                if (res.toUpperCase().equals("EXIT")) {
                    break;
                } else {
                    System.out.println(res);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
