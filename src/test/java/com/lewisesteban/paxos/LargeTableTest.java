package com.lewisesteban.paxos;

import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.paxosnode.listener.SnapshotManager;
import com.lewisesteban.paxos.storage.virtual.InterruptibleVirtualFileAccessor;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;
import junit.framework.TestCase;
import largetable.Client;
import largetable.Server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static com.lewisesteban.paxos.NetworkFactory.initSimpleNetwork;

public class LargeTableTest extends TestCase {

    public void testSimpleRequests() {
        List<PaxosNetworkNode> cluster = initSimpleNetwork(3, new Network(), stateMachineList(3));
        largetable.Client<PaxosServer> client = new Client<>(cluster.stream().map(PaxosNetworkNode::getPaxosSrv).collect(Collectors.toList()), "client");
        client.put("keyA", "valA");
        client.put("keyB", "valB");
        client.append("keyA", "+");
        assertEquals("valA+", client.get("keyA"));
        assertEquals("valB", client.get("keyB"));
    }

    public void testSnapshot() throws InterruptedException {
        SnapshotManager.SNAPSHOT_FREQUENCY = 2;

        Network network = new Network();
        List<PaxosNetworkNode> cluster = initSimpleNetwork(3, network, stateMachineList(3));
        largetable.Client<PaxosServer> client = new Client<>(cluster.stream().map(PaxosNetworkNode::getPaxosSrv).collect(Collectors.toList()), "client");
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

    private List<Callable<StateMachine>> stateMachineList(int size) {
        List<Callable<StateMachine>> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            final int nodeId = i;
            list.add(() -> new Server(InterruptibleVirtualFileAccessor.creator(nodeId)));
        }
        return list;
    }
}
