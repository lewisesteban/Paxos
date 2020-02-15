package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.PaxosTestCase;
import com.lewisesteban.paxos.paxosnode.membership.Bully;
import com.lewisesteban.paxos.paxosnode.membership.NodeStateSupervisor;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.*;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.virtual.InterruptibleVirtualFileAccessor;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;
import largetable.Client;
import largetable.LargeTableClient;
import largetable.Server;

import java.io.IOException;
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

    public void testSwitchProposer() throws StorageException, Client.LargeTableException, InterruptedException {
        NodeStateSupervisor.GOSSIP_AVG_TIME_PER_NODE = 10;
        NodeStateSupervisor.FAILURE_TIMEOUT = 40;
        Bully.WAIT_AFTER_FAILURE = 10;
        Bully.WAIT_FOR_VICTORY_TIMEOUT = 50;

        int[] nbProposeReceived = { 0, 0, 0 };

        Network network = new Network();
        List<PaxosNetworkNode> cluster = initSimpleNetwork(3, network, stateMachineList(3));
        LargeTableClient<NodeMonitor> client = new LargeTableClient<>(
                cluster.stream()
                        .map(netNode -> new NodeMonitor(netNode.getPaxosSrv(),
                                null, () -> {
                            if (network.isRunning(netNode.getAddress()))
                                nbProposeReceived[netNode.getPaxosSrv().getId()] += 1;
                        }))
                        .collect(Collectors.toList()),
                "client", InterruptibleVirtualFileAccessor.creator(-1));

        // first two requests, second one has to be on leader
        Thread.sleep(200); // wait for election
        client.append("key", "a");
        client.append("key", "b");
        assertTrue(nbProposeReceived[0] <= 1);
        assertTrue(nbProposeReceived[1] <= 1);
        assertTrue(nbProposeReceived[0] + nbProposeReceived[1] <= 1);
        assertTrue(nbProposeReceived[2] >= 1 && nbProposeReceived[2] <= 2);
        int node0originalNb = nbProposeReceived[0];
        int node1originalNb = nbProposeReceived[1];
        int node2originalNb = nbProposeReceived[2];

        // kill 2, do request on 0 or 1
        Thread.sleep(100); // let 2 finish scattering
        network.kill(addr(2));
        System.out.println(System.lineSeparator() + "# 2 killed");
        Thread.sleep(200); // wait for election
        client.append("key", "c");
        if (nbProposeReceived[0] > node0originalNb) {
            assertEquals(node0originalNb + 1, nbProposeReceived[0]);
            assertEquals(node1originalNb, nbProposeReceived[1]);
        } else {
            assertEquals(node0originalNb, nbProposeReceived[0]);
            assertEquals(node1originalNb + 1, nbProposeReceived[1]);
        }
        int node0postKillNb = nbProposeReceived[0];
        int node1postKillNb = nbProposeReceived[1];

        // restore 2, next request should be on old leader 1
        network.start(addr(2));
        Thread.sleep(200); // wait for election
        client.append("key", "d");
        assertEquals(node0postKillNb, nbProposeReceived[0]);
        assertEquals(node1postKillNb + 1, nbProposeReceived[1]);
        assertEquals(node2originalNb, nbProposeReceived[2]);

        // now client is redirected to 2
        int node2postRestoreNb = nbProposeReceived[2];
        client.append("key", "e");
        assertEquals(node0postKillNb, nbProposeReceived[0]);
        assertEquals(node1postKillNb + 1, nbProposeReceived[1]);
        assertTrue(nbProposeReceived[2] > node2postRestoreNb); // allow extra for catching up

        assertEquals("abcde", client.get("key"));
    }

    public void testClientEnd() throws Exception {
        NodeStateSupervisor.GOSSIP_AVG_TIME_PER_NODE = 10;
        NodeStateSupervisor.FAILURE_TIMEOUT = 40;
        Bully.WAIT_AFTER_FAILURE = 10;
        Bully.WAIT_FOR_VICTORY_TIMEOUT = 50;

        int[] nbEndReceived = { 0, 0, 0 };

        Network network = new Network();
        List<PaxosNetworkNode> cluster = initSimpleNetwork(3, network, stateMachineList(3));
        LargeTableClient<NodeMonitor> client = new LargeTableClient<>(
                cluster.stream()
                        .map(netNode -> new NodeMonitor(netNode.getPaxosSrv(),
                                () -> {
                            if (network.isRunning(addr(netNode.getPaxosSrv().getId())))
                                nbEndReceived[netNode.getPaxosSrv().getId()]++;
                        }, null))
                        .collect(Collectors.toList()),
                "client", InterruptibleVirtualFileAccessor.creator(-1));

        // first command
        Thread.sleep(200); // wait for leader election
        System.out.println(System.lineSeparator() + "cmd1");
        client.append("key", "a"); // do command to random server; client will then be given leader's id
        System.out.println(System.lineSeparator() + "cmd2");
        client.append("key", "b"); // do command to leader
        assertTrue(nbEndReceived[0] + nbEndReceived[1] + nbEndReceived[2] <= 1); // can be 1 if client's original first request was not to leader
        assertEquals(0, nbEndReceived[2]);
        int node0originalNb = nbEndReceived[0];
        int node1originalNb = nbEndReceived[1];

        // kill leader, do a command to another server
        network.kill(addr(2));
        Thread.sleep(200); // wait for leader election
        System.out.println(System.lineSeparator() + "cmd3");
        client.append("key", "c"); // this command may be sent to either 0 or 1, and may cause redirection
        client.append("key", "d"); // this command will be sent to 1
        int node0postKillNb = node0originalNb;
        if (nbEndReceived[0] > node0originalNb) {
            // "c" command arrived on 0, and client is redirected
            assertEquals(node0originalNb + 1, nbEndReceived[0]);
            node0postKillNb += 1;
        } else {
            // command arrived on 1, no redirection
            assertEquals(node0originalNb, nbEndReceived[0]);
        }
        assertEquals(node1originalNb, nbEndReceived[1]);
        assertEquals(0, nbEndReceived[2]);

        // restore leader, do a command to leader
        network.start(addr(2));
        Thread.sleep(200); // wait for leader election
        System.out.println(System.lineSeparator() + "cmd4");
        client.append("key", "e");
        client.append("key", "f");
        assertEquals(node0postKillNb, nbEndReceived[0]);
        assertEquals(node1originalNb + 1, nbEndReceived[1]);
        int node1PostRestoreNb = node1originalNb + 1;
        int node2PostRestoreNb = nbEndReceived[2];

        // manual end()
        client.end();
        assertEquals(node0postKillNb, nbEndReceived[0]);
        assertEquals(node1PostRestoreNb, nbEndReceived[1]);
        assertEquals(node2PostRestoreNb + 1, nbEndReceived[2]);
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

    class NodeMonitor implements PaxosProposer, RemotePaxosNode {
        private Runnable clientEndOverride;
        private Runnable proposeOverride;
        private PaxosServer base;

        NodeMonitor(PaxosServer base, Runnable clientEndOverride, Runnable proposeOverride) {
            this.clientEndOverride = clientEndOverride;
            this.proposeOverride = proposeOverride;
            this.base = base;
        }

        @Override
        public long getNewInstanceId() throws IOException {
            return base.getNewInstanceId();
        }

        @Override
        public Result propose(Command command, long instanceId) throws IOException {
            if (proposeOverride != null)
                proposeOverride.run();
            return base.propose(command, instanceId);
        }

        @Override
        public void endClient(String clientId) throws IOException {
            if (clientEndOverride != null)
                clientEndOverride.run();
            base.endClient(clientId);
        }

        @Override
        public int getId() {
            return base.getId();
        }

        @Override
        public int getFragmentId() {
            return base.getFragmentId();
        }

        @Override
        public AcceptorRPCHandle getAcceptor() {
            return base.getAcceptor();
        }

        @Override
        public ListenerRPCHandle getListener() {
            return base.getListener();
        }

        @Override
        public MembershipRPCHandle getMembership() {
            return base.getMembership();
        }
    }
}
