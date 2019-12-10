package clientapp;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.PaxosNode;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.*;
import com.lewisesteban.paxos.storage.SafeSingleFileStorage;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.WholeFileAccessor;
import largetable.Client;
import largetable.LargeTableClient;
import largetable.Server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class TerminalApp {

    public static void main(String[] args) {
        try {
            readInputAndExecute(initialize(args));
            System.exit(0);
        } catch (StorageException e) {
            System.err.println("ERR storage. " + e.getMessage());
            System.exit(1);
        } catch (Client.LargeTableException e) {
            System.err.println("ERR network. " + e.getMessage());
            System.exit(1);
        }
    }

    private static LargeTableClient initialize(String[] args) throws StorageException, Client.LargeTableException {
        List<PaxosTestServer> cluster = new ArrayList<>();
        cluster.add(new PaxosTestServer(0));
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

    static class PaxosTestServer implements RemotePaxosNode, PaxosProposer {
        PaxosNode paxosNode;

        PaxosTestServer(int nodeId) throws StorageException {
            List<RemotePaxosNode> cluster = new ArrayList<>();
            cluster.add(this);
            paxosNode = new PaxosNode(nodeId, 0, cluster,
                    new Server(WholeFileAccessor::new),
                    (file, dir) -> new SafeSingleFileStorage(file, dir, WholeFileAccessor::new),
                    WholeFileAccessor::new);
            paxosNode.start();
        }

        @Override
        public long getNewInstanceId() throws IOException {
            return paxosNode.getNewInstanceId();
        }

        @Override
        public Result propose(Command command, long instanceId) throws IOException {
            return paxosNode.propose(command, instanceId);
        }

        @Override
        public void endClient(String clientId) throws IOException {
            paxosNode.endClient(clientId);
        }

        @Override
        public int getId() {
            return 0;
        }

        @Override
        public int getFragmentId() {
            return 0;
        }

        @Override
        public AcceptorRPCHandle getAcceptor() {
            return paxosNode.getAcceptor();
        }

        @Override
        public ListenerRPCHandle getListener() {
            return paxosNode.getListener();
        }

        @Override
        public MembershipRPCHandle getMembership() {
            return paxosNode.getMembership();
        }
    }
}
