package apps;

import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.WholeFileAccessor;
import largetable.Client;
import largetable.LargeTableClient;
import network.NodeClient;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;

public class TerminalApp {

    // TODO check failure handling and client recovery
    // TODO when servers go down and then back up, it seems client can't reconnect
    // TODO "last" should not do anything if "end" has been called

    public static void main(String[] args) {
        try {
            readInputAndExecute(initialize(args));
            System.exit(0);
        } catch (StorageException | FileNotFoundException e) {
            System.err.println("ERR storage. " + e.getMessage());
            System.exit(1);
        } catch (Client.LargeTableException e) {
            System.err.println("ERR network. " + e.getMessage());
            System.exit(1);
        } catch (RemoteException | NotBoundException e) {
            System.err.println("ERR RMI. " + e.getMessage());
            e.printStackTrace();
        } catch (FileFormatException e) {
            System.err.println(e.getMessage());
        }
    }

    private static LargeTableClient initialize(String[] args) throws StorageException, Client.LargeTableException, RemoteException, NotBoundException, FileNotFoundException, FileFormatException {
        List<NodeClient> cluster = NetworkFileParser.createRemoteNodes(args.length <= 1 ? null : args[1]);
        LargeTableClient client = new LargeTableClient<>(cluster, "app", WholeFileAccessor::new);
        client.recover();
        return client;
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
