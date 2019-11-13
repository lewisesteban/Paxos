package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.PaxosTestCase;
import com.lewisesteban.paxos.client.CommandException;
import com.lewisesteban.paxos.client.SingleFragmentClient;
import com.lewisesteban.paxos.paxosnode.membership.Bully;
import com.lewisesteban.paxos.paxosnode.membership.NodeStateSupervisor;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import static com.lewisesteban.paxos.NetworkFactory.*;

public class ElectionTest extends PaxosTestCase {

    public void testBasicElection() throws InterruptedException, IOException {
        NodeStateSupervisor.GOSSIP_AVG_TIME_PER_NODE = 20;
        NodeStateSupervisor.FAILURE_TIMEOUT = 100;
        Bully.WAIT_AFTER_FAILURE = 20;
        Bully.WAIT_FOR_VICTORY_TIMEOUT = 1000;

        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(4, network, stateMachinesEmpty(4));
        PaxosProposer proposer = nodes.get(0).getPaxosSrv();
        Thread.sleep(200);
        assertNull(nodes.get(3).getPaxosSrv().propose(cmd1, 0).getExtra());
        Result result = proposer.propose(cmd2, 1);
        assertEquals(Result.BAD_PROPOSAL, result.getStatus());
        assertEquals((Integer)3, result.getExtra().getLeaderId());

        System.out.println("--- killing node");
        network.kill(addr(3));
        Thread.sleep(200);
        assertEquals((Integer)2, proposer.propose(cmd3, 2).getExtra().getLeaderId());
        System.out.println("+++ restarting node");
        network.start(addr(3));
        Thread.sleep(200);
        assertEquals((Integer)3, proposer.propose(cmd4, 3).getExtra().getLeaderId());
    }

    public void testElectionRandom() throws InterruptedException {
        NodeStateSupervisor.GOSSIP_AVG_TIME_PER_NODE = 20;
        NodeStateSupervisor.FAILURE_TIMEOUT = 80;
        Bully.WAIT_AFTER_FAILURE = 20;
        Bully.WAIT_FOR_VICTORY_TIMEOUT = 100;

        final int NB_NODES = 5;
        final int NB_ROUNDS = 20;

        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(NB_NODES, network, stateMachinesEmpty(NB_NODES));

        for (int round = 0; round < NB_ROUNDS; round++) {
            System.out.println();
            System.out.println("############ new round");

            // set the network in a random state, and wait for election
            System.out.println(">>>>>> killing/restoring phase");
            Thread thread = serialKiller(network, nodes, 200, 80);
            thread.start();
            thread.join();
            System.out.println(">>>>>> election phase");
            StringBuilder stringBuilder = new StringBuilder("alive: ");
            for (int node = 0; node < nodes.size(); ++node) {
                if (nodes.get(node).isRunning())
                   stringBuilder.append(node).append(" ");
            }
            System.out.println(stringBuilder.toString());
            Thread.sleep(200);

            // count nb of nodes alive
            int nodesAlive = 0;
            for (PaxosNetworkNode node : nodes) {
                if (node.isRunning())
                    nodesAlive++;
            }
            if (nodesAlive <= NB_NODES / 2) {
                System.out.println("NOT ENOUGH NODES ALIVE");
                continue;
            }

            // get highest live node, which should be the leader
            PaxosNetworkNode highestNumberedLiveNode = null;
            for (int node = NB_NODES - 1; node >= 0 && highestNumberedLiveNode == null; --node) {
                if (nodes.get(node).isRunning())
                    highestNumberedLiveNode = nodes.get(node);
            }
            assert highestNumberedLiveNode != null;
            int leaderId = highestNumberedLiveNode.getPaxosSrv().getId();
            //System.out.println("leader=" + leaderId);

            // check every node's leader
            try {
                Result result = highestNumberedLiveNode.getPaxosSrv().propose(cmd1, 0);
                assertNull(result.getExtra());
                for (int node = 0; node < leaderId; ++node) {
                    if (nodes.get(node).isRunning()) {
                        //System.out.println("checking " + node + "...");
                        result = nodes.get(node).getPaxosSrv().propose(cmd1, 0);
                        assertEquals((Integer)leaderId, result.getExtra().getLeaderId());
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                fail();
            }
        }
    }

    public void testNetworkPartitioning() throws InterruptedException, IOException {
        NodeStateSupervisor.GOSSIP_AVG_TIME_PER_NODE = 20;
        NodeStateSupervisor.FAILURE_TIMEOUT = 100;
        Bully.WAIT_AFTER_FAILURE = 100;
        Bully.WAIT_FOR_VICTORY_TIMEOUT = 100;

        // create the partitioned network
        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(7, 2, network, stateMachinesEmpty(7));
        network.disconnectRack(0);
        System.out.println(">>>>>>>>>> start (partitioned)");
        Thread.sleep(200);

        int majorityRack = getRacks(nodes)[0].length >= 4 ? 0 : 1;
        int majorityRackLeader = 0;
        for (int node = 0; node < nodes.size(); ++node) {
            if (nodes.get(node).getRack() == majorityRack && node > majorityRackLeader)
                majorityRackLeader = node;
        }

        // make sure majority rack has a fixed leader
        for (int node = 0; node < 7; node++) {
            if (nodes.get(node).getRack() == majorityRack) {
                if (node != majorityRackLeader) {
                    Result result = nodes.get(node).getPaxosSrv().propose(cmd1, 0);
                    assertEquals((Integer)majorityRackLeader, result.getExtra().getLeaderId());
                }
            }
        }

        // make sure minority rack doesn't have a leader from the other rack
        for (int node = 0; node < 7; node++) {
            if (nodes.get(node).getRack() != majorityRack) {
                Result result = nodes.get(node).getPaxosSrv().propose(cmd1, 0);
                assertTrue(result.getExtra() == null
                        || nodes.get(result.getExtra().getLeaderId()).getRack() != majorityRack);
            }
        }

        // put them back together and make sure node 6 is the leader
        System.out.println(">>>>>>>>>> reconnect rack");
        network.reconnectRack(0);
        Thread.sleep(300);
        for (int node = 0; node < 6; node++) {
            System.out.println("checking " + node);
            Result result = nodes.get(node).getPaxosSrv().propose(cmd1, 0);
            assertEquals((Integer)6, result.getExtra().getLeaderId());
        }
    }

    public void testClientRedirection() throws IOException, InterruptedException {
        NodeStateSupervisor.GOSSIP_AVG_TIME_PER_NODE = 20;
        NodeStateSupervisor.FAILURE_TIMEOUT = 100;

        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(7, 2, network, stateMachinesMirror(7));
        Thread.sleep(200); // wait for consensus

        // test failure of request on wrong server and success of request on correct server
        Result res = nodes.get(0).getPaxosSrv().propose(cmd1, nodes.get(0).getPaxosSrv().getNewInstanceId());
        int leader = res.getExtra().getLeaderId();
        res = nodes.get(leader).getPaxosSrv().propose(cmd1, nodes.get(leader).getPaxosSrv().getNewInstanceId());
        assertEquals(0, res.getInstanceId());
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, res.getStatus());
        assertNull(res.getExtra());

        // test client
        System.out.println(">>>>> test with good network");
        List<PaxosProposer> proposers = nodes.stream().map(PaxosNetworkNode::getPaxosSrv).collect(Collectors.toList());
        for (int trial = 0; trial < 3; trial++) {
            SingleFragmentClient client = new SingleFragmentClient(proposers, "client" + trial, null);
            try {
                Serializable resData = client.tryCommand(cmd2);
                assertEquals(cmd2.getData().toString(), resData.toString());
            } catch (CommandException e) {
                e.printStackTrace();
                fail();
            }
        }

        // kill a few servers and make sure it still works
        System.out.println(">>>>> test with three failed nodes");
        network.kill(addr(6));
        network.kill(addr(5));
        network.kill(addr(0));
        for (int trial = 0; trial < 3; trial++) {
            SingleFragmentClient client = new SingleFragmentClient(proposers, "client" + trial, null);
            try {
                Serializable resData = client.tryCommand(cmd3);
                assertEquals(cmd3.getData().toString(), resData.toString());
            } catch (CommandException e) {
                e.printStackTrace();
                fail();
            }
        }

        // kill one server too much and make sure I get an exception
        network.kill(addr(1));
        try {
            new SingleFragmentClient(proposers, "client666", null).tryCommand(cmd4);
            fail();
        } catch (CommandException ignored) { }
    }
}
