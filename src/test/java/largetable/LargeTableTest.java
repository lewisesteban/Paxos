package largetable;

import com.lewisesteban.paxos.PaxosTestCase;
import com.lewisesteban.paxos.client.ClientCommandSender;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.paxosnode.listener.SnapshotManager;
import com.lewisesteban.paxos.paxosnode.listener.UnneededInstanceGossipper;
import com.lewisesteban.paxos.paxosnode.membership.Membership;
import com.lewisesteban.paxos.paxosnode.membership.NodeStateSupervisor;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.*;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.virtual.InterruptibleVirtualFileAccessor;
import com.lewisesteban.paxos.storage.virtual.VirtualFileSystem;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.ClientNetNode;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.lewisesteban.paxos.NetworkFactory.initSimpleNetwork;

public class LargeTableTest extends PaxosTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        VirtualFileSystem.clear();
    }

    public void testSimpleRequests() throws Exception {
        List<PaxosNetworkNode> cluster = initSimpleNetwork(3, new Network(), stateMachineList(3));
        largetable.Client<PaxosServer> client = new Client<>(
                cluster.stream().map(PaxosNetworkNode::getPaxosSrv).collect(Collectors.toList()),
                "client", InterruptibleVirtualFileAccessor.creator(-1));
        client.put("keyA", "valA");
        client.put("keyB", "valB");
        client.append("keyA", "+");
        assertEquals("valA+", client.get("keyA"));
        assertEquals("valB", client.get("keyB"));
    }

    public void testSnapshot() throws Exception {
        SnapshotManager.SNAPSHOT_FREQUENCY = 2;

        Network network = new Network();
        List<PaxosNetworkNode> cluster = initSimpleNetwork(3, network, stateMachineList(3));
        largetable.Client<PaxosServer> client = new Client<>(
                cluster.stream().map(PaxosNetworkNode::getPaxosSrv).collect(Collectors.toList()),
                "client", InterruptibleVirtualFileAccessor.creator(-1));
        client.put("keyA", "valA");
        client.get("keyA");
        client.get("keyA");
        client.get("keyA");
        Thread.sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 50);
        client.get("keyA");
        Thread.sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 50);

        network.killAll();
        network.startAll();

        assertEquals("valA", client.get("keyA"));
        assertTrue(InterruptibleVirtualFileAccessor.creator(0).create("acceptor0", null).listFiles().length < 6);
    }

    public void testEndClientSnapshot() throws Exception {
        SnapshotManager.SNAPSHOT_FREQUENCY = 2;

        Network network = new Network();
        List<PaxosNetworkNode> cluster = initSimpleNetwork(3, network, stateMachineList(3));
        largetable.Client<PaxosServer> clientA = new Client<>(
                cluster.stream().map(PaxosNetworkNode::getPaxosSrv).collect(Collectors.toList()),
                "clientA", InterruptibleVirtualFileAccessor.creator(-1));
        largetable.Client<PaxosServer> clientB = new Client<>(
                cluster.stream().map(PaxosNetworkNode::getPaxosSrv).collect(Collectors.toList()),
                "clientB", InterruptibleVirtualFileAccessor.creator(-1));
        clientA.put("keyA", "valA");
        clientB.get("keyA");
        clientB.get("keyA");
        clientB.get("keyA");
        Thread.sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 50);
        clientB.get("keyA");
        Thread.sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 50);
        assertEquals(5, InterruptibleVirtualFileAccessor.creator(0).create("acceptor0", null).listFiles().length);

        clientA.close();
        clientB.get("keyA");
        Thread.sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 50);
        clientB.get("keyA");
        Thread.sleep(500);
        assertTrue(InterruptibleVirtualFileAccessor.creator(0).create("acceptor0", null).listFiles().length < 7);
    }

    public void testClientRecovery() throws Exception {
        AtomicInteger receivedCmds = new AtomicInteger(0);
        AtomicBoolean stateMachineFinished = new AtomicBoolean(false);
        Server server = new Server(InterruptibleVirtualFileAccessor.creator(0)) {
            @Override
            public Serializable execute(Serializable data) {
                receivedCmds.incrementAndGet();
                try {
                    if (receivedCmds.get() == 0)
                       Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                stateMachineFinished.set(true);
                return super.execute(data);
            }
        };
        List<Callable<StateMachine>> stateMachineList = new ArrayList<>();
        stateMachineList.add(() -> server);
        Network network = new Network();
        List<PaxosNetworkNode> cluster = initSimpleNetwork(1, network, stateMachineList);

        // start and then kill client
        ClientNetNode clientNetNode = new ClientNetNode(-1);
        largetable.Client<PaxosServer> client = new Client<>(
                cluster.stream().map(PaxosNetworkNode::getPaxosSrv).collect(Collectors.toList()),
                "client", InterruptibleVirtualFileAccessor.creator(-1));
        Thread clientThread = new Thread(() -> client.put("key", "val"));
        clientThread.start();
        Thread.sleep(50);
        clientNetNode.kill();

        // new client should recover initiated operation
        Client newClient = new Client<>(
                cluster.stream().map(PaxosNetworkNode::getPaxosSrv).collect(Collectors.toList()),
                "client", InterruptibleVirtualFileAccessor.creator(-1));
        assertEquals(1, receivedCmds.get());
        assertTrue(stateMachineFinished.get());
        assertEquals("val", newClient.get("key"));
        assertEquals(2, receivedCmds.get());
    }

    public void testClientCrashStress() throws InterruptedException {
        testClientCrashStress(false, 5000);
    }

    public void testClientServerCrashStress() throws InterruptedException {
        testClientCrashStress(true, 3000);
    }

    private void testClientCrashStress(boolean serverFailures, int time) throws InterruptedException {
        SnapshotManager.SNAPSHOT_FREQUENCY = 20;
        UnneededInstanceGossipper.GOSSIP_FREQUENCY = 100;
        NodeStateSupervisor.GOSSIP_AVG_TIME_PER_NODE = 100;
        NodeStateSupervisor.FAILURE_TIMEOUT = 300;

        final int NB_SERVERS = 5;

        // state machines that check new received values are always superior
        AtomicBoolean error = new AtomicBoolean(false);
        List<Callable<StateMachine>> stateMachines = new ArrayList<>();
        for (int i = 0; i < NB_SERVERS; i++) {
            final int nodeId = i;
            stateMachines.add(() -> new Server(InterruptibleVirtualFileAccessor.creator(nodeId)) {
                @Override
                public Serializable execute(Serializable data) {
                    Command receivedCmd = (Command) data;
                    if (receivedCmd.getType() != Command.GET) {
                        String oldVal = (String) execute(new Command(Command.GET, receivedCmd.getData()));
                        if (oldVal != null) {
                            if (Integer.parseInt(oldVal) >= Integer.parseInt(receivedCmd.getData()[1])) {
                                error.set(true);
                                System.err.println("error: server " + nodeId + " received key=" + receivedCmd.getData()[0] + " val=" + receivedCmd.getData()[1] + " after val=" + oldVal);
                            }
                        }
                    }
                    return super.execute(data);
                }
            });
        }

        // initialize servers
        Network network = new Network();
        List<PaxosNetworkNode> cluster = initSimpleNetwork(NB_SERVERS, network, stateMachines);

        // client working thread
        AtomicInteger clientIdInt = new AtomicInteger(-1);
        AtomicInteger lastCmdId = new AtomicInteger(0);
        Runnable startClient = () -> {
            clientIdInt.decrementAndGet();
            System.out.println("=== client starting: nb " + clientIdInt.get());
            List<ClientToSrvConnection> paxosServers = cluster.stream().map(node -> new ClientToSrvConnection(network, node.getPaxosSrv(), clientIdInt.get(), node.getAddress())).collect(Collectors.toList());
            Client<ClientToSrvConnection> client;
            try {
                ClientNetNode netNode = new ClientNetNode(clientIdInt.get());
                network.addNode(netNode);
                network.start(ClientNetNode.address(clientIdInt.get()));
                client = new Client<>(paxosServers, "client", InterruptibleVirtualFileAccessor.creator(clientIdInt.get()));
                Thread clientWorkingThread = new Thread(() -> {
                    while (netNode.isRunning()) {
                        lastCmdId.incrementAndGet();
                        System.out.println("client " + netNode.getAddress().getNodeIdInCluster() + " trying " + lastCmdId.get());
                        client.put("key", Integer.toString(lastCmdId.get()));
                        System.out.println("client " + netNode.getAddress().getNodeIdInCluster() + " finished: " + lastCmdId.get());
                    }
                });
                clientWorkingThread.start();
            } catch (StorageException e) {
                e.printStackTrace();
            }
        };

        // client killing/restoring thread
        Thread clientKiller = new Thread(() -> {
            final Random random = new Random();
            long startTime = System.currentTimeMillis();
            startClient.run();
            while (System.currentTimeMillis() - startTime < time) {
                try {
                    Thread.sleep(random.nextInt(300));
                    network.kill(ClientNetNode.address(clientIdInt.get()));
                    Thread.sleep(200);
                    System.out.println("starting client...");
                    startClient.run();
                    System.out.println("client finished");
                } catch (InterruptedException ignored) { }
            }
        });


        // start test
        clientKiller.start();
        if (serverFailures) {
            Thread serverKiller = serialKiller(network, cluster, time, 400);
            serverKiller.start();
            serverKiller.join();
            for (int srv = 0; srv < NB_SERVERS; srv++)
                network.start(addr(srv));
        }
        clientKiller.join();
        assertFalse(error.get()); // TODO error (with server failures)
    }

    public void testSerialization() throws StorageException, InterruptedException {
        SnapshotManager.SNAPSHOT_FREQUENCY = 15;
        UnneededInstanceGossipper.GOSSIP_FREQUENCY = 60;
        NodeStateSupervisor.GOSSIP_AVG_TIME_PER_NODE = 80;
        NodeStateSupervisor.FAILURE_TIMEOUT = 400;

        final int NB_CLIENTS = 10;
        final int NB_SERVERS = 5;
        final int NB_ENTRIES_PER_CLIENT = 1;
        final int TIME = 5000;

        // init network
        Map<Integer, Runnable> stateMachinesKillSwitches = new TreeMap<>();
        Map<Integer, StateMachine> stateMachines = new TreeMap<>();
        List<Callable<StateMachine>> stateMachineCreators = new ArrayList<>();
        for (int i = 0; i < NB_SERVERS; i++) {
            final int nodeId = i;
            stateMachineCreators.add(() -> new Server(InterruptibleVirtualFileAccessor.creator(nodeId)) {
                boolean keepGoing = true;

                @Override
                public void setup(int nodeId) throws IOException {
                    stateMachines.put(nodeId, this);
                    stateMachinesKillSwitches.put(nodeId, () -> {
                        synchronized (this) {
                            keepGoing = false;
                        }
                    });
                    super.setup(nodeId);
                }

                @Override
                public void applySnapshot(Snapshot snapshot) throws StorageException {
                    synchronized (this) {
                        if (keepGoing)
                            super.applySnapshot(snapshot);
                    }
                }

                @Override
                public void applyCurrentWaitingSnapshot() throws StorageException {
                    synchronized (this) {
                        if (keepGoing)
                            super.applyCurrentWaitingSnapshot();
                    }
                }
            });
        }
        Network network = new Network();
        List<PaxosNetworkNode> cluster = initSimpleNetwork(NB_SERVERS, network, stateMachineCreators);
        List<PaxosServer> paxosServers = cluster.stream().map(PaxosNetworkNode::getPaxosSrv).collect(Collectors.toList());

        // init clients
        AtomicBoolean clientError = new AtomicBoolean(false);
        List<TestClient> clients = new ArrayList<>();
        for (int i = 0; i < NB_CLIENTS; ++i) {
            clients.add(new TestClient("client" + i, paxosServers, NB_ENTRIES_PER_CLIENT, clientError));
        }

        // start
        Thread serverKiller = serialKiller(network, cluster, TIME, 500, (node) -> stateMachinesKillSwitches.get(node).run());
        serverKiller.start();
        clients.forEach(TestClient::start);
        serverKiller.join();

        // finish
        System.out.println("finish");
        cluster.forEach(node -> { if (!node.isRunning()) network.start(node.getAddress()); });
        clients.forEach(TestClient::stop);
        //Logger.set(true);
        long start = System.currentTimeMillis();
        clients.forEach(TestClient::join);
        System.out.println("join: " + (System.currentTimeMillis() - start) + " ms");
        assertFalse(clientError.get());

        // make sure everybody is up to date
        Membership.LEADER_ELECTION = false;
        cluster.forEach(node -> {
            try {
                System.out.println("sending request to " + node.getPaxosSrv().getId());
                new ClientCommandSender(null).doCommand(node.getPaxosSrv(),
                        new com.lewisesteban.paxos.paxosnode.Command(new Command(Command.GET, new String[] { "key" }), "lastClient", 0));
            } catch (Throwable e) {
                e.printStackTrace(); // TODO CommandFailedException (may be related to the proposer having a runing command, which, after waiting for its end, has no entry in Listener.ExecutedCommand)
                fail();
            }
        });

        // check server states
        System.out.println("checking...");
        Map<String, String> globalData = new TreeMap<>();
        clients.forEach(client -> client.getData().forEach(globalData::put));
        stateMachines.forEach((nodeId, sm) -> globalData.forEach((key, val) -> {
            String smVal = (String) sm.execute(new Command(Command.GET, new String[] { key }));
            if ((val != null || smVal != null) && (val == null || !val.equals(smVal))) {
                System.err.println("server " + nodeId + " key=" + key + " val=" + smVal + " but should be " + val);
                fail();
            }
        }));
    }

    private List<Callable<StateMachine>> stateMachineList(int size) {
        List<Callable<StateMachine>> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            final int nodeId = i;
            list.add(() -> new Server(InterruptibleVirtualFileAccessor.creator(nodeId)));
        }
        return list;
    }

    private class TestClient {
        private Random random = new Random();
        private Thread thread;
        private Client<PaxosServer> client;
        private Map<String, String> data = new TreeMap<>();
        private int nbEntries;
        private String clientId;
        private AtomicBoolean error;
        private boolean keepGoing = true;

        TestClient(String id, List<PaxosServer> servers, int nbEntries, AtomicBoolean error) throws StorageException {
            client = new Client<>(servers, id, InterruptibleVirtualFileAccessor.creator(-1));
            this.nbEntries = nbEntries;
            this.clientId = id;
            this.error = error;
        }

        void stop() {
            keepGoing = false;
        }

        void start() {
            thread = new Thread(() -> {
                while (keepGoing) {
                    String key = clientId + "_key" + random.nextInt(nbEntries);
                    String val = Integer.toString(random.nextInt(10));
                    if (random.nextInt(10) > 3) {
                        // append
                        if (data.get(key) == null)
                            data.put(key, val);
                        else
                            data.put(key, data.get(key).concat(val));
                        client.append(key, val);
                        System.out.println("client " + clientId + " append key " + key + " val " + val + " success");
                    } else {
                        if (random.nextInt(3) > 0) {
                            // get (and check)
                            val = client.get(key);
                            String myVal = data.get(key);
                            if ((val != null || myVal != null) && (val == null || !val.equals(myVal))) {
                                System.err.println("error: client " + clientId + " key " + key + " is " + val + " but should be " + data.get(key));
                                error.set(true);
                            }
                            System.out.println("client " + clientId + " get " + key + " val=" + val + " OK");
                        } else {
                            // put
                            data.put(key, val);
                            client.put(key, val);
                            System.out.println("client " + clientId + " put key " + key + " val " + val + " success");
                        }
                    }
                }
            });
            thread.start();
        }

        void join() {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        Map<String, String> getData() {
            return data;
        }
    }

    private class ClientToSrvConnection implements PaxosProposer, RemotePaxosNode {
        private Network network;
        private PaxosServer paxosServer;
        private Network.Address address;
        private Network.Address targetAddress;

        ClientToSrvConnection(Network network, PaxosServer paxosServer, int clientId, Network.Address targetAddress) {
            this.network = network;
            this.paxosServer = paxosServer;
            this.address = ClientNetNode.address(clientId);
            this.targetAddress = targetAddress;
        }

        @Override
        public long getNewInstanceId() throws IOException {
            return network.tryNetCall(() -> paxosServer.getNewInstanceId(), address, targetAddress);
        }

        @Override
        public Result propose(com.lewisesteban.paxos.paxosnode.Command command, long instanceId) throws IOException {
            return network.tryNetCall(() -> paxosServer.propose(command, instanceId), address, targetAddress);
        }

        @Override
        public void endClient(String clientId) throws IOException {
            AtomicReference<IOException> exception = new AtomicReference<>(null);
            network.tryNetCall(() -> {
                try {
                    paxosServer.endClient(clientId);
                } catch (IOException e) {
                    exception.set(e);
                }
            }, address, targetAddress);
            if (exception.get() != null)
                throw exception.get();
        }

        @Override
        public int getId() {
            return paxosServer.getId();
        }

        @Override
        public int getFragmentId() {
            return paxosServer.getFragmentId();
        }

        @Override
        public AcceptorRPCHandle getAcceptor() {
            return paxosServer.getAcceptor();
        }

        @Override
        public ListenerRPCHandle getListener() {
            return paxosServer.getListener();
        }

        @Override
        public MembershipRPCHandle getMembership() {
            return paxosServer.getMembership();
        }
    }
}
