package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.PaxosTestCase;
import com.lewisesteban.paxos.storage.virtual.InterruptibleVirtualFileAccessor;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;
import largetable.LargeTableClient;
import largetable.Server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static com.lewisesteban.paxos.NetworkFactory.initSimpleNetwork;
import static largetable.Client.ExecutedCommand.TYPE_APPEND;
import static largetable.Client.ExecutedCommand.TYPE_GET;

public class ClientTest extends PaxosTestCase {

    public void testRecover() throws Exception {
        List<PaxosNetworkNode> cluster = initSimpleNetwork(3, new Network(), stateMachineList(3));
        LargeTableClient<PaxosServer> client = new LargeTableClient<>(
                cluster.stream().map(PaxosNetworkNode::getPaxosSrv).collect(Collectors.toList()),
                "client", InterruptibleVirtualFileAccessor.creator(-1));

        assertNull(client.recover());

        client.put("key", "val");
        client.append("key", "1");
        assertEquals("val1", client.get("key"));

        // start new client and recover
        client = new LargeTableClient<>(
                cluster.stream().map(PaxosNetworkNode::getPaxosSrv).collect(Collectors.toList()),
                "client", InterruptibleVirtualFileAccessor.creator(-2));
        LargeTableClient.ExecutedCommand lastCmd = client.recover();
        assertEquals(TYPE_GET, lastCmd.getCmdType());
        assertEquals("key", lastCmd.getCmdKey());
        assertEquals("val1", lastCmd.getResult());

        // check recover returns last command, and doesn't re-execute
        client.append("key", "2");
        client.recover();
        lastCmd = client.recover();
        assertEquals(TYPE_APPEND, lastCmd.getCmdType());
        assertEquals("key", lastCmd.getCmdKey());
        assertEquals("2", lastCmd.getCmdValue());
    }

    public void testTryAgain() throws Exception {
        Network network = new Network();
        List<PaxosNetworkNode> cluster = initSimpleNetwork(3, network, stateMachineList(3));
        LargeTableClient<PaxosServer> client = new LargeTableClient<>(
                cluster.stream().map(PaxosNetworkNode::getPaxosSrv).collect(Collectors.toList()),
                "client", InterruptibleVirtualFileAccessor.creator(-1));

        client.put("unusedKey", "val");
        client.put("key", "val");
        network.kill(addr(0));
        network.kill(addr(1));

        // check tryAgain successfully finished last cmd
        try {
            client.get("key");
            fail();
        } catch (LargeTableClient.LargeTableException e) {
            network.start(addr(0));
            assertEquals("val", client.tryAgain());
        }

        // check tryAgain doesn't execute a second time
        client.append("key", "1");
        client.tryAgain();
        assertEquals("val1", client.get("key"));
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
