package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.PaxosTestCase;
import com.lewisesteban.paxos.paxosnode.membership.NodeStateSupervisor;
import com.lewisesteban.paxos.virtualnet.Network;

import static com.lewisesteban.paxos.NetworkFactory.initSimpleNetwork;
import static com.lewisesteban.paxos.NetworkFactory.stateMachinesEmpty;

public class ElectionTest extends PaxosTestCase {

    public void test() throws InterruptedException {
        NodeStateSupervisor.GOSSIP_FREQUENCY = 10;
        NodeStateSupervisor.FAILURE_TIMEOUT = 80;
        Network network = new Network();
        initSimpleNetwork(7, network, stateMachinesEmpty(7));
        Thread.sleep(1000);
        System.out.println("--- killing node 0");
        network.kill(0);
        Thread.sleep(1000);
    }
}
