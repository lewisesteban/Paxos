package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.PaxosTestCase;
import com.lewisesteban.paxos.paxosnode.membership.Bully;
import com.lewisesteban.paxos.paxosnode.membership.NodeStateSupervisor;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;

import java.io.IOException;
import java.util.List;

import static com.lewisesteban.paxos.NetworkFactory.initSimpleNetwork;
import static com.lewisesteban.paxos.NetworkFactory.stateMachinesEmpty;

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
        assertEquals(3, result.getExtra().getLeaderId());

        System.out.println("--- killing node");
        network.kill(3);
        Thread.sleep(200);
        assertEquals(2, proposer.propose(cmd3, 2).getExtra().getLeaderId());
        System.out.println("+++ restarting node");
        network.start(3);
        Thread.sleep(200);
        assertEquals(3, proposer.propose(cmd4, 3).getExtra().getLeaderId());
    }

    public void testElectionRandom() throws InterruptedException {
        NodeStateSupervisor.GOSSIP_AVG_TIME_PER_NODE = 20;
        NodeStateSupervisor.FAILURE_TIMEOUT = 80;
        Bully.WAIT_AFTER_FAILURE = 20;
        Bully.WAIT_FOR_VICTORY_TIMEOUT = 100;

        final int NB_NODES = 5;
        final int NB_ROUNDS = 100;

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
                        assertEquals(leaderId, result.getExtra().getLeaderId());
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                fail();
            }
        }
    }

    // TODO partition test

    // TODO for other tests: use client that doesn't respect elections + set very high gossip and bully times
}
