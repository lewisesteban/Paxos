package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.NetworkFactory;
import com.lewisesteban.paxos.PaxosTestCase;
import com.lewisesteban.paxos.client.BasicPaxosClient;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.lewisesteban.paxos.NetworkFactory.stateMachinesSame;
import static com.lewisesteban.paxos.NetworkFactory.stateMachinesSingle;

public class HeavyPaxosTest extends PaxosTestCase {

    /**
     * Should be executed with no dedicated proposer.
     */
    public void testSingleStateMachineNoFailure() throws Exception {
        final int NB_NODES = 7;
        final int NB_CLIENTS = 100; // used to take less than 20 seconds (500 clients, 3 requests, 7 machines)
        final int NB_REQUESTS = 3;

        AtomicBoolean error = new AtomicBoolean(false);
        int[] lastReceived = new int[NB_CLIENTS];
        for (int i = 0; i < lastReceived.length; ++i)
            lastReceived[i] = -1;
        StateMachine stateMachine = data -> {
            TestCommand cmdData = (TestCommand) data;
            if (cmdData.cmdNb != lastReceived[cmdData.clientId] + 1) {
                error.set(true);
                System.err.println("client number " + cmdData.cmdNb + " got " + cmdData.cmdNb + " after " + lastReceived[cmdData.clientId]);
            } else {
                lastReceived[cmdData.clientId] = cmdData.cmdNb;
            }
            return null;
        };

        Network network = new Network();
        network.setWaitTimes(0, 0, 1, 0);
        List<PaxosNetworkNode> nodes = NetworkFactory.initSimpleNetwork(NB_NODES, network, stateMachinesSingle(() -> stateMachine, NB_NODES));

        Thread[] clients = new Thread[NB_CLIENTS];
        for (int clientId = 0; clientId < NB_CLIENTS; ++clientId) {
            final int thisClientsId = clientId;
            clients[clientId] = new Thread(() -> {
                PaxosProposer paxosServer = nodes.get(new Random().nextInt(nodes.size())).getPaxosSrv();
                BasicPaxosClient paxosHandle = new BasicPaxosClient(paxosServer, "client" + thisClientsId);
                for (int cmdId = 0; cmdId < NB_REQUESTS; cmdId++) {
                    TestCommand cmdData = new TestCommand(thisClientsId, cmdId);
                    try {
                        paxosHandle.doCommand(cmdData);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        for (Thread client : clients) {
            client.start();
        }
        for (Thread client : clients) {
            client.join();
            assertFalse(error.get());
        }
    }

    public void testBasicClientsNoFailure() throws Exception {
        final int NB_NODES = 7;
        final int NB_CLIENTS = 100; // used to take less than 20 seconds (500 clients, 3 requests, 7 machines)
        final int NB_REQUESTS = 3;

        StateMachine stateMachine = data -> data;
        Network network = new Network();
        network.setWaitTimes(0, 0, 1, 0);
        List<PaxosNetworkNode> nodes = NetworkFactory.initSimpleNetwork(NB_NODES, network, stateMachinesSame(() -> stateMachine, NB_NODES));

        AtomicBoolean error = new AtomicBoolean(false);
        Thread[] clients = new Thread[NB_CLIENTS];
        for (int clientId = 0; clientId < NB_CLIENTS; ++clientId) {
            final int thisClientsId = clientId;
            clients[clientId] = new Thread(() -> {
                PaxosProposer paxosServer = nodes.get(new Random().nextInt(nodes.size())).getPaxosSrv();
                BasicPaxosClient paxosHandle = new BasicPaxosClient(paxosServer, "client" + thisClientsId);
                for (int cmdId = 0; cmdId < NB_REQUESTS; cmdId++) {
                    TestCommand cmdData = new TestCommand(thisClientsId, cmdId);
                    try {
                        Serializable res = paxosHandle.doCommand(cmdData);
                        TestCommand resCmd = (TestCommand)res;
                        if (resCmd.clientId != thisClientsId || resCmd.cmdNb != cmdId) {
                            error.set(true);
                            System.err.println("client " + thisClientsId + " received " + resCmd + " instead of " + cmdData);
                        }
                    } catch (IOException e) {
                        error.set(true);
                        e.printStackTrace();
                    }
                }
            });
        }

        for (Thread client : clients) {
            client.start();
        }
        for (Thread client : clients) {
            client.join();
            assertFalse(error.get());
        }
    }

    class TestCommand implements java.io.Serializable {

        TestCommand(int clientId, int cmdNb) {
            this.clientId = clientId;
            this.cmdNb = cmdNb;
        }

        int clientId;
        int cmdNb;
        @Override
        public String toString() {
            return "client" + clientId + ";cmd" + cmdNb;
        }

    }
}
