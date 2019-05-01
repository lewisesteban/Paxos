package com.lewisesteban.paxos;

import com.lewisesteban.paxos.rpc.NodeRPCHandle;
import junit.framework.*;

import java.util.ArrayList;
import java.util.List;

public class Test extends TestCase {

    public void test() {
        List<NodeRPCHandle> nodes = new ArrayList<NodeRPCHandle>();
        PaxosNode node0 = new PaxosNode(0, nodes);
        nodes.add(node0);
        nodes.add(new PaxosNode(1, nodes));
        assert node0.propose("ONE");
        assert !node0.propose("TWO");
    }
}
