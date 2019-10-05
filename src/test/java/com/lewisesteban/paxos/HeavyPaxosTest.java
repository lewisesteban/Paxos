package com.lewisesteban.paxos;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.lewisesteban.paxos.NetworkFactory.executorsSingle;
import static com.lewisesteban.paxos.NetworkFactory.initSimpleNetwork;

public class HeavyPaxosTest extends TestCase {

    /**
     * Should be executed with no dedicated proposer.
     */
    public void testManyClientsNoFailure() throws Exception {
        final int NB_NODES = 7;
        final int NB_CLIENTS = 100;
        final int NB_REQUESTS = 100;

        Client[] clients = new Client[NB_CLIENTS];
        StateMachine stateMachine = (data) -> {
            Command cmdData = (Command)data;
            clients[cmdData.getClientId()].receive(cmdData);
            return null;
        };
        Network network = new Network();
        network.setWaitTimes(0, 0, 1, 0);
        List<PaxosNetworkNode> nodes = initSimpleNetwork(NB_NODES, network, executorsSingle(stateMachine, NB_NODES));

        for (int clientId = 0; clientId < NB_CLIENTS; ++clientId) {
            final int thisClientsId = clientId;
            clients[clientId] = new Client(clientId, nodes, new Thread(() -> {
                for (int cmdId = 0; cmdId < NB_REQUESTS; cmdId++) {
                    Command cmdData = new Command(thisClientsId, cmdId, null);
                    clients[thisClientsId].propose(new Command(thisClientsId, cmdId, cmdData));
                }
            }));
        }

        for (Client client : clients) {
            client.getThread().start();
        }
        for (Client client : clients) {
            client.getThread().join();
            assertFalse(client.getError());
            assertEquals(client.lastReceived(), NB_REQUESTS - 1);
        }
    }

    private class Client {
        private int id;
        private Thread thread;
        private long lastReceivedCommand = -1;
        private boolean error = false;
        private List<PaxosNetworkNode> nodes;
        private Random random = new Random();
        private PaxosServer paxosServer = null;
        private Map<Object, Integer> instances = new HashMap<>();

        Client(int id, List<PaxosNetworkNode> nodes, Thread thread) {
            this.id = id;
            this.thread = thread;
            this.nodes = nodes;
            findPaxosServer();
        }

        private void findPaxosServer() {
            paxosServer = nodes.get(random.nextInt(nodes.size())).getPaxosSrv();
        }

        private void propose(Command data) {
            boolean success = false;
            Result res = null;
            while (!success) {
                try {
                    res = paxosServer.proposeNew(data);
                    success = res.getSuccess();
                    if (!success) {
                        findPaxosServer();
                        Logger.println("FAIL inst " + res.getInstanceId() + " command " + data);
                    }
                } catch (IOException e) {
                    findPaxosServer();
                }
            }
            instances.put(data, res.getInstanceId());
        }

        void receive(Command clientCommand) {
            if (clientCommand.getCommandNb() != expected()) {
                error = true;
                System.err.println("Client nb " + id + " got " + clientCommand.getCommandNb() + " instead of " + expected() + "\tinstance=" + instances.get(clientCommand));
            } else if (Logger.isOn()) {
                Logger.println("++ Client nb " + id + " got " + clientCommand.getCommandNb());
            }
            lastReceivedCommand = clientCommand.getCommandNb();
        }

        long expected() {
            return lastReceivedCommand + 1;
        }

        long lastReceived() {
            return lastReceivedCommand;
        }

        boolean getError() {
            return error;
        }

        Thread getThread() {
            return thread;
        }
    }
}
