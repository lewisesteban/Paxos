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

    public void test() throws InterruptedException, IOException {
        NodeStateSupervisor.GOSSIP_FREQUENCY = 10;
        NodeStateSupervisor.FAILURE_TIMEOUT = 100;
        Bully.WAIT_AFTER_FAILURE = 20;
        Bully.WAIT_FOR_VICTORY_TIMEOUT = 1000;

        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(4, network, stateMachinesEmpty(4));
        PaxosProposer proposer = nodes.get(0).getPaxosSrv();
        Thread.sleep(500);
        assertNull(nodes.get(3).getPaxosSrv().propose(cmd1, 0).getExtra());
        Result result = proposer.propose(cmd2, 1);
        assertEquals(Result.BAD_PROPOSAL, result.getStatus());
        assertEquals(3, result.getExtra().getLeaderId());

        System.out.println("--- killing node");
        network.kill(3);
        Thread.sleep(500);
        assertEquals(2, proposer.propose(cmd3, 2).getExtra().getLeaderId());
        System.out.println("+++ restarting node");
        network.start(3);
        Thread.sleep(500);
        assertEquals(3, proposer.propose(cmd4, 3).getExtra().getLeaderId());
    }
}
