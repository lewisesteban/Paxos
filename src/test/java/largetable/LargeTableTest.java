package largetable;

import com.lewisesteban.paxos.PaxosTestCase;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.paxosnode.listener.SnapshotManager;
import com.lewisesteban.paxos.paxosnode.listener.UnneededInstanceGossipper;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
        Thread.sleep(100);
        client.get("keyA");
        Thread.sleep(100);

        network.killAll();
        network.startAll();

        assertEquals("valA", client.get("keyA"));
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
        testClientCrashStress(false, 3000);
    }

    public void testClientServerCrashStress() throws InterruptedException {
        testClientCrashStress(true, 2000); // when a client starts just before a node is killed, it will wait a long time
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
                    while (true) {
                        lastCmdId.incrementAndGet();
                        System.out.println("client trying " + lastCmdId.get());
                        client.put("key", Integer.toString(lastCmdId.get()));
                        System.out.println("client success: " + lastCmdId.get());
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
                    startClient.run();
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
        assertFalse(error.get());
    }

    public void testSerialization() {
        // clients don't crash, servers do
        // clients do random operations on a number of entries, but mostly appends
        // clients have their own entries, and don't overlap
        // servers write executed operation on disk
        // at the end, check serialized execution of ops of each client equals the state of the server
    }

    private List<Callable<StateMachine>> stateMachineList(int size) {
        List<Callable<StateMachine>> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            final int nodeId = i;
            list.add(() -> new Server(InterruptibleVirtualFileAccessor.creator(nodeId)));
        }
        return list;
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
            network.tryNetCall(() -> {
                try {
                    paxosServer.endClient(clientId);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }, address, targetAddress);
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
