package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.NetworkFactory;
import com.lewisesteban.paxos.PaxosTestCase;
import com.lewisesteban.paxos.client.BasicPaxosClient;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.lewisesteban.paxos.NetworkFactory.*;

public class HeavyPaxosTestNoDedicatedProposer extends PaxosTestCase {
    private Random random = new Random();

    public void testBasicClientsNoFailure() throws Exception {
        final int NB_NODES = 7;
        final int NB_CLIENTS = 100;
        final int NB_REQUESTS = 3;

        StateMachine stateMachine = basicStateMachine(data -> data).call();
        Network network = new Network();
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
                        Serializable res = paxosHandle.tryDoCommand(cmdData);
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

    public void testSingleStateMachineNoFailure() throws Exception {
        final int NB_NODES = 7;
        final int NB_CLIENTS = 200;
        final int NB_REQUESTS = 5;

        AtomicBoolean error = new AtomicBoolean(false);
        Network network = new Network();
        List<PaxosNetworkNode> nodes = NetworkFactory.initSimpleNetwork(NB_NODES, network,
                stateMachinesSingle(TestStateMachine.creator(NB_CLIENTS, error), NB_NODES));

        Thread[] clients = new Thread[NB_CLIENTS];
        for (int clientId = 0; clientId < NB_CLIENTS; ++clientId) {
            final int thisClientsId = clientId;
            clients[clientId] = new Thread(() -> {
                PaxosProposer paxosServer = nodes.get(new Random().nextInt(nodes.size())).getPaxosSrv();
                BasicPaxosClient paxosHandle = new BasicPaxosClient(paxosServer, "client" + thisClientsId);
                for (int cmdId = 0; cmdId < NB_REQUESTS; cmdId++) {
                    TestCommand cmdData = new TestCommand(thisClientsId, cmdId);
                    try {
                        paxosHandle.tryDoCommand(cmdData);
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

    public void testServerFailures() throws InterruptedException {
        final int NB_NODES = 5;
        final int NB_CLIENTS = 10;
        final int TIME = 10000;
        final int KILLER_MAX_SLEEP = 100;

        AtomicBoolean keepGoing = new AtomicBoolean(true);
        AtomicBoolean error = new AtomicBoolean(false);

        final Network network = new Network();
        Iterable<Callable<StateMachine>> stateMachines = TestStateMachine.creatorList(NB_CLIENTS, error, NB_NODES);
        List<PaxosNetworkNode> nodes = NetworkFactory.initSimpleNetwork(NB_NODES, network, stateMachines);

        final Thread serialKiller = new Thread(() -> {
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < TIME) {
                try {
                    Thread.sleep(random.nextInt(KILLER_MAX_SLEEP));
                } catch (InterruptedException ignored) {
                }
                int server = random.nextInt(NB_NODES);
                if (nodes.get(server).isRunning()) {
                    System.out.println("--- killing " + server);
                    TestStateMachine.stop(server);
                    network.kill(server);
                } else {
                    System.out.println("+++ restoring " + server);
                    network.start(server);
                }
            }
        });

        Thread[] clients = new Thread[NB_CLIENTS];
        for (int clientId = 0; clientId < NB_CLIENTS; ++clientId) {
            final int thisClientsId = clientId;
            final int nodeId = new Random().nextInt(nodes.size());
            final PaxosProposer paxosServer = nodes.get(nodeId).getPaxosSrv();
            final BasicPaxosClient paxosHandle = new BasicPaxosClient(paxosServer, "client" + thisClientsId);
            clients[clientId] = new Thread(() -> {
                int cmdId = 0;
                while (keepGoing.get() && !error.get()) {
                    TestCommand cmdData = new TestCommand(thisClientsId, cmdId);
                    paxosHandle.doCommand(cmdData);
                    System.out.println("YEAH finished cmd " + cmdId);
                    cmdId++;
                }
            });
        }

        serialKiller.start();
        for (Thread client : clients)
            client.start();

        serialKiller.join(); // time's up
        keepGoing.set(false);
        for (PaxosNetworkNode node : nodes) {
            if (!node.isRunning())
                node.start();
        }
        for (Thread client : clients)
            client.join();

        if (error.get())
            fail();
    }

    /*public void testNetworkFailures() {

    }*/

    static class TestStateMachine extends BasicStateMachine {
        int[] lastReceived;
        AtomicBoolean error;
        private boolean stopped = false;

        static final Map<Integer, TestStateMachine> runningStateMachines = new TreeMap<>();

        TestStateMachine(int nbClients, AtomicBoolean error) {
            lastReceived = new int[nbClients];
            for (int i = 0; i < nbClients; ++i) {
                lastReceived[i] = -1;
            }
            this.error = error;
        }

        TestStateMachine(int nbClients, AtomicBoolean error, int nodeNb) {
            synchronized (runningStateMachines) {
                runningStateMachines.put(nodeNb, this);
            }
            lastReceived = new int[nbClients];
            for (int i = 0; i < nbClients; ++i) {
                lastReceived[i] = -1;
            }
            this.error = error;
        }

        void stop() {
            stopped = true;
        }

        @Override
        public Serializable execute(Serializable data) {
            if (stopped)
                return null;
            TestCommand command = (TestCommand) data;
            if (command.cmdNb != lastReceived[command.clientId] + 1) {
                error.set(true);
                System.err.println("error: client nb " + command.clientId + " expected cmd nb " +
                        (lastReceived[command.clientId] + 1) + " got " + command.cmdNb);
            } else {
                lastReceived[command.clientId] = command.cmdNb;
            }
            return data;
        }

        static Callable<StateMachine> creator(int nbClients, AtomicBoolean error) {
            return () -> new TestStateMachine(nbClients, error);
        }

        static Iterable<Callable<StateMachine>> creatorList(int nbClients, AtomicBoolean error, int nbNodes) {
            List<Callable<StateMachine>> list = new ArrayList<>();
            for (int nodeNb = 0; nodeNb < nbNodes; ++nodeNb) {
                final int thisNodeNb = nodeNb;
                list.add(() -> new TestStateMachine(nbClients, error, thisNodeNb));
            }
            return list;
        }

        static void stop(int nodeNb) {
            synchronized (runningStateMachines) {
                runningStateMachines.get(nodeNb).stop();
                runningStateMachines.remove(nodeNb);
            }
        }
    }

    static class TestCommand implements java.io.Serializable {

        TestCommand(int clientId, int cmdNb) {
            this.clientId = clientId;
            this.cmdNb = cmdNb;
        }

        int clientId;
        int cmdNb;

        @Override
        public String toString() {
            return "client" + clientId + "cmd" + cmdNb;
        }
    }
}
