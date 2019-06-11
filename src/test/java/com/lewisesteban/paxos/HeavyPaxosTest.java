package com.lewisesteban.paxos;

import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;
import junit.framework.TestCase;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import static com.lewisesteban.paxos.NetworkFactory.*;

public class HeavyPaxosTest extends TestCase {

    public void testManyClientsNoFailure() throws Exception {
        final int NB_NODES = 7;
        final int NB_CLIENTS = 10;
        final int NB_REQUESTS = 100;

        Client[] clients = new Client[NB_CLIENTS];
        Executor executor = (i, data) -> {
            Command cmd = (Command)data;
            clients[cmd.clientId].receive(cmd.commandId);
        };
        Network network = new Network();
        network.setWaitTimes(0, 0, 1, 0);
        List<PaxosNetworkNode> nodes = initSimpleNetwork(NB_NODES, network, executorsSingle(executor, NB_NODES));
        PaxosServer dedicatedServer = nodes.get(0).getPaxosSrv();

        for (int clientId = 0; clientId < NB_CLIENTS; ++clientId) {
            final int thisClientsId = clientId;
            clients[clientId] = new Client(clientId, new Thread(() -> {
                for (int cmdId = 0; cmdId < NB_REQUESTS; cmdId++) {
                    try {
                        dedicatedServer.proposeNew(new Command(thisClientsId, cmdId));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
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

    private class Command implements Serializable {
        int clientId;
        int commandId;

        Command(int clientId, int commandId) {
            this.clientId = clientId;
            this.commandId = commandId;
        }
    }

    private class Client {
        private int id;
        private Thread thread;
        private int lastReceivedCommand = -1;
        private boolean error = false;

        Client(int id, Thread thread) {
            this.id = id;
            this.thread = thread;
        }

        void receive(int data) {
            if (data != expected()) {
                error = true;
                System.err.println("Client nb " + id + " got " + data + " instead of " + expected());
            }
            lastReceivedCommand = data;
        }

        int expected() {
            return lastReceivedCommand + 1;
        }

        int lastReceived() {
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
