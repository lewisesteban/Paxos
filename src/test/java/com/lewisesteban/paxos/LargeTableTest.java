package com.lewisesteban.paxos;

import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.paxosnode.listener.SnapshotManager;
import com.lewisesteban.paxos.storage.virtual.InterruptibleVirtualFileAccessor;
import com.lewisesteban.paxos.storage.virtual.VirtualFileSystem;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.ClientNetNode;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;
import junit.framework.TestCase;
import largetable.Client;
import largetable.Server;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.lewisesteban.paxos.NetworkFactory.initSimpleNetwork;

public class LargeTableTest extends TestCase {

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

    private List<Callable<StateMachine>> stateMachineList(int size) {
        List<Callable<StateMachine>> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            final int nodeId = i;
            list.add(() -> new Server(InterruptibleVirtualFileAccessor.creator(nodeId)));
        }
        return list;
    }
}
